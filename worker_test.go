package gototo

import "testing"

type SampleType struct {
	String string `json:"string"`
	Bool   bool   `json:"bool"`
	Int    int    `json:"int"`
	Data   struct {
		A int    `json:"a"`
		B string `json:"b"`
	} `json:"data"`
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
}
