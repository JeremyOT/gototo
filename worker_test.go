package gototo

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"
)

type SampleType struct {
	String string `json:"string"`
	Bool   bool   `json:"bool"`
	Int    int    `json:"int"`
	Data   struct {
		A int    `json:"a"`
		B string `json:"b"`
	} `json:"data"`
}

type SampleValidatedType struct {
	String string `json:"string"`
}

func (s *SampleValidatedType) Validate() (err error) {
	if s.String == "" {
		err = fmt.Errorf("Empty String field")
	}
	return
}

type SampleUnpackerType struct {
	String string `json:"string"`
}

func (s *SampleUnpackerType) UnpackRequest(input interface{}) (err error) {
	if inputMap, ok := input.(map[string]string); !ok {
		err = fmt.Errorf("Bad input type: %s", input)
	} else {
		s.String = inputMap["string"]
	}
	return
}

func checkPanics(t *testing.T, f func()) {
	defer func() {
		if err := recover(); err == nil {
			t.Error("Expected panic")
		}
	}()
	f()
}

func TestConvert(t *testing.T) {

	workerFunc := func(converted *SampleType) *Response {
		if converted.String != "hi" {
			t.Error("Wrong String", converted.String)
		}
		if converted.Int != 42 {
			t.Error("Wrong Int", converted.Int)
		}
		if converted.Bool != true {
			t.Error("Wrong Bool", converted.Bool)
		}
		if converted.Data.A != 4242 {
			t.Error("Wrong Data.A", converted.Data.A)
		}
		if converted.Data.B != "Bye" {
			t.Error("Wrong Data.B", converted.Data.B)
		}
		return &Response{Success: true}
	}
	missingWorkerFunc := func(converted *SampleType) *Response {
		if converted.String != "" {
			t.Error("Wrong String", converted.String)
		}
		if converted.Int != 0 {
			t.Error("Wrong Int", converted.Int)
		}
		if converted.Bool != false {
			t.Error("Wrong Bool", converted.Bool)
		}
		if converted.Data.A != 0 {
			t.Error("Wrong Data.A", converted.Data.A)
		}
		if converted.Data.B != "" {
			t.Error("Wrong Data.B", converted.Data.B)
		}
		return &Response{Success: true}
	}
	w := New("", 0)
	w.MakeWorkerFunction(workerFunc)(map[string]interface{}{
		"string": "hi",
		"bool":   true,
		"int":    42,
		"data": map[interface{}]interface{}{
			"a": 4242,
			"b": "Bye",
		},
	})
	w.MakeWorkerFunction(workerFunc)(map[string]interface{}{
		"string": "hi",
		"bool":   true,
		"int":    42,
		"data": map[interface{}]interface{}{
			"a": 4242,
			"b": "Bye",
			"c": "Ignored",
		},
		"bad": "Ignored",
	})
	w.MakeWorkerFunction(missingWorkerFunc)(map[string]interface{}{})

	checkPanics(t, func() {
		w.MakeWorkerFunction(1)
	})
	checkPanics(t, func() {
		w.MakeWorkerFunction(func(i string) string { return "" })
	})
	checkPanics(t, func() {
		w.MakeWorkerFunction(func(i ...*Response) *Response { return nil })
	})
	checkPanics(t, func() {
		w.MakeWorkerFunction(func(i *Response, j *Response) *Response { return nil })
	})
	checkPanics(t, func() {
		w.MakeWorkerFunction(func(i *Response) (*Response, *Response) { return nil, nil })
	})
	checkPanics(t, func() {
		w.MakeWorkerFunction(func(i *string) *Response { return nil })
	})
	validatingFunc := w.MakeWorkerFunction(func(i *SampleValidatedType) *Response { return nil })
	if r := validatingFunc(map[string]interface{}{"string": "test"}).(*Response); r != nil {
		t.Error("Unexpected non nil response:", r)
	}
	if r := validatingFunc(map[string]interface{}{}).(*Response); r == nil || r.Success {
		t.Error("Expected error:", r)
	}
	unpackingFunc := w.MakeWorkerFunction(func(i *SampleUnpackerType) *Response { return nil })
	if r := unpackingFunc(map[string]string{"string": "test"}).(*Response); r != nil {
		t.Error("Unexpected non nil response:", r)
	}
	if r := unpackingFunc(map[string]interface{}{"string": 42}).(*Response); r == nil || r.Success {
		t.Error("Expected error:", r)
	}
	d := &struct{ Time time.Duration }{Time: 5 * time.Second}
	out, err := json.Marshal(d)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(out))
	var s struct{ Time time.Duration }
	json.Unmarshal(out, &s)
	fmt.Println(s)
}

func TestCall(t *testing.T) {
	worker := New("tcp://*:64242", 10)
	worker.RegisterWorkerFunction("test_func", worker.MakeWorkerFunction(func(i *SampleValidatedType) *Response { return &Response{Success: true, Result: i} }))
	err := worker.Start()
	if err != nil {
		t.Fatal("Failed to start worker:", err)
	}
	time.Sleep(100 * time.Millisecond)
	defer worker.Shutdown()
	connection := NewConnection("tcp://127.0.0.1:64242")
	connection.RegisterResponseType("test_func", &Response{})
	err = connection.Start()
	if err != nil {
		t.Fatal("Failed to start connection:", err)
	}
	defer connection.Stop()
	resp, err := connection.Call("test_func", &SampleValidatedType{String: "test request string"})
	if err != nil {
		t.Fatal("Failed to decode response:", err)
	}
	if r, ok := resp.(*Response); !ok {
		t.Fatal("Bad response:", r)
	} else {
		if r.Success != true {
			t.Fatal("Bad response:", r)
		}
		if svti, err := connection.ConvertValue(reflect.TypeOf(SampleValidatedType{}), r.Result); err != nil {
			t.Fatal("Bad response:", r, err)
		} else {
			if svt, ok := svti.(*SampleValidatedType); !ok {
				t.Fatal("Bad response:", r, svti)
			} else if svt.String != "test request string" {
				t.Fatal("Bad response:", r)
			}
		}
	}
}
