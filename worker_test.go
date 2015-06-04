package gototo

import (
	"reflect"
	"testing"
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

func TestConvert(t *testing.T) {

	workerFunc := func(in interface{}) interface{} {
		converted, ok := in.(*SampleType)
		if !ok {
			t.Fatalf("Failed to convert type: %#v\n", in)
		}
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
		return true
	}
	missingWorkerFunc := func(in interface{}) interface{} {
		converted, ok := in.(*SampleType)
		if !ok {
			t.Fatalf("Failed to convert type: %#v\n", in)
		}
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
		return true
	}
	w := New("", 0)
	w.ConvertFunctionType(reflect.TypeOf(SampleType{}), workerFunc)(map[string]interface{}{
		"string": "hi",
		"bool":   true,
		"int":    42,
		"data": map[interface{}]interface{}{
			"a": 4242,
			"b": "Bye",
		},
	})
	w.ConvertFunctionType(reflect.TypeOf(SampleType{}), workerFunc)(map[string]interface{}{
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
	w.ConvertFunctionType(reflect.TypeOf(SampleType{}), missingWorkerFunc)(map[string]interface{}{})
}
