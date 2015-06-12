package gototo

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
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

type WaitRequest struct {
	Timeout time.Duration `json:"timeout"`
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
	tcpAddr, _ := net.ResolveTCPAddr("tcp", ":0")
	l, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		t.Fatal("Failed to create TCP listener:", err)
	}
	tcpAddr = l.Addr().(*net.TCPAddr)
	err = l.Close()
	if err != nil {
		t.Fatal("Failed to create TCP listener:", err)
	}
	worker := New(fmt.Sprintf("tcp://*:%d", tcpAddr.Port), 10)
	worker.RegisterWorkerFunction("test_func", worker.MakeWorkerFunction(func(i *SampleValidatedType) *Response {
		if i.String != "fail" {
			return CreateSuccessResponse(i)
		} else {
			return CreateErrorResponse(errors.New("Empty string!"))
		}
	}))
	worker.RegisterWorkerFunction("slow_test_func", worker.MakeWorkerFunction(func(i *WaitRequest) *Response {
		time.Sleep(i.Timeout)
		return CreateSuccessResponse(i)
	}))
	err = worker.Start()
	if err != nil {
		t.Fatal("Failed to start worker:", err)
	}
	time.Sleep(100 * time.Millisecond)
	defer worker.Stop()
	addr := fmt.Sprintf("tcp://127.0.0.1:%d", tcpAddr.Port)
	connection := NewConnection(addr)
	connection.RegisterResponseType("test_func", &SampleValidatedType{}, true)
	connection.RegisterResponseType("slow_test_func", &WaitRequest{}, true)
	connection.RegisterDefaultOptions("slow_test_func", &RequestOptions{Timeout: 100 * time.Millisecond, RetryCount: 3})
	err = connection.Start()
	if err != nil {
		t.Fatal("Failed to start connection:", err)
	}
	err = connection.Disconnect(addr)
	if err != nil {
		t.Fatal("Failed to disconnect:", err)
	}
	if len(connection.GetEndpoints()) > 0 {
		t.Fatal("Expected no connections")
	}
	err = connection.Connect(addr)
	if err != nil {
		t.Fatal("Failed to reconnect:", err)
	}
	if len(connection.GetEndpoints()) != 1 {
		t.Fatal("Expected one connections")
	}
	err = connection.SetEndpoints()
	if err != nil {
		t.Fatal("Failed to disconnect:", err)
	}
	if len(connection.GetEndpoints()) > 0 {
		t.Fatal("Expected no connections")
	}
	err = connection.SetEndpoints(addr)
	if err != nil {
		t.Fatal("Failed to reconnect:", err)
	}
	if len(connection.GetEndpoints()) != 1 {
		t.Fatal("Expected one connections")
	}
	err = connection.Connect(addr)
	if err != nil {
		t.Fatal("Failed to reconnect:", err)
	}
	if len(connection.GetEndpoints()) != 1 {
		t.Fatal("Expected one connections")
	}
	defer connection.Stop()
	resp, err := connection.Call("test_func", &SampleValidatedType{String: "test request string"})
	if err != nil {
		t.Fatal("Failed to decode response:", err)
	}
	if svt, ok := resp.(*SampleValidatedType); !ok {
		t.Fatalf("Bad response: %#v\n", resp)
	} else if svt.String != "test request string" {
		t.Fatalf("Bad response: %#v\n", resp)
	}
	resp, err = connection.Call("test_func", &SampleValidatedType{String: ""})
	if err == nil {
		t.Fatalf("Expected error: %#v %#v\n", resp, err)
	} else {
		if responseError, ok := err.(*ResponseError); !ok {
			t.Fatalf("Expected *ResponseError: %#v\n", err)
		} else if responseError.Error() != "Validation failed: Empty String field" {
			t.Fatalf("Expected 'Validation failed: Empty String field': %#v\n", err)
		}
	}
	resp, err = connection.Call("test_func", &SampleValidatedType{String: "fail"})
	if err == nil {
		t.Fatalf("Expected error: %#v %#v\n", resp, err)
	} else {
		if responseError, ok := err.(*ResponseError); !ok {
			t.Fatalf("Expected *ResponseError: %#v\n", err)
		} else if responseError.Error() != "Empty string!" {
			t.Fatalf("Expected 'Empty string!': %#v\n", err)
		}
	}
	resp, err = connection.Call("slow_test_func", &WaitRequest{Timeout: 200 * time.Millisecond})
	if err != nil {
		t.Error("Unexpected response error:", err)
	}
	resp, err = connection.Call("slow_test_func", &WaitRequest{Timeout: 1 * time.Second})
	if err != ErrTimeout {
		t.Error("Expected timeout, found:", err, resp)
	}
}
