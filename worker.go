package gototo

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	"github.com/mitchellh/mapstructure"
)

type DecodeHookFunc interface{}

// DecoderConfig is the configuration that is used to create a new decoder
// and allows customization of various aspects of decoding.
type DecoderConfig struct {
	// DecodeHook, if set, will be called before any decoding and any
	// type conversion (if WeaklyTypedInput is on). This lets you modify
	// the values before they're set down onto the resulting struct.
	//
	// If an error is returned, the entire decode will fail with that
	// error.
	DecodeHook DecodeHookFunc

	// If ErrorUnused is true, then it is an error for there to exist
	// keys in the original map that were unused in the decoding process
	// (extra keys).
	ErrorUnused bool

	// ZeroFields, if set to true, will zero fields before writing them.
	// For example, a map will be emptied before decoded values are put in
	// it. If this is false, a map will be merged.
	ZeroFields bool

	// If WeaklyTypedInput is true, the decoder will make the following
	// "weak" conversions:
	//
	//   - bools to string (true = "1", false = "0")
	//   - numbers to string (base 10)
	//   - bools to int/uint (true = 1, false = 0)
	//   - strings to int/uint (base implied by prefix)
	//   - int to bool (true if value != 0)
	//   - string to bool (accepts: 1, t, T, TRUE, true, True, 0, f, F,
	//     FALSE, false, False. Anything else is an error)
	//   - empty array = empty map and vice versa
	//   - negative numbers to overflowed uint values (base 10)
	//
	WeaklyTypedInput bool

	// Result is a pointer to the struct that will contain the decoded
	// value.
	Result interface{}

	// The tag name that mapstructure reads for field names. This
	// defaults to "mapstructure"
	TagName string
}

// WorkerFunction may be registered with a worker
type WorkerFunction func(interface{}) interface{}

// MarshalFunction converts the result of a WorkerFunction to a byte slice to be
// sent back to the caller.
type MarshalFunction func(interface{}) ([]byte, error)

// UnmarshalFunction converts a byte slice to a worker invocation payload.
type UnmarshalFunction func([]byte) (*Request, error)

func UnmarshalJSON(buf []byte) (data *Request, err error) {
	err = json.Unmarshal(buf, &data)
	return
}

// Response is a general response container that will be returned with Success = false
// if any errors are encountered while attempting to call a type safe function.
type Response struct {
	Success bool        `json:"success"`
	Error   string      `json:"error,omitempty"`
	Result  interface{} `json:"result,omitempty"`
}

// CreateErrorResponse creates a new response based on an error. If err is nil, then
// the response signify success. Otherwise it will be initialized with the error
// text.
func CreateErrorResponse(err error) *Response {
	if err == nil {
		return &Response{Success: true}
	} else {
		return &Response{Success: false, Error: err.Error()}
	}
}

// CreateSuccessResponse creates a new successful response with the supplied parameter
// stored in its Result field.
func CreateSuccessResponse(result interface{}) *Response {
	return &Response{Success: true, Result: result}
}

// CreateResponse creates a new response based on the supplied result and error. If
// err is nil, then the response will contain the supplied result. Otherwise it will
// be initialized with the error text.
func CreateResponse(result interface{}, err error) *Response {
	if err == nil {
		return &Response{Success: true, Result: result}
	} else {
		return &Response{Success: false, Error: err.Error()}
	}
}

// Request is a general request container. It has a Method identifier and Parameters.
type Request struct {
	Method     string      `json:"method"`
	Parameters interface{} `json:"parameters"`
}

// Validator allows automatic validation when calling type safe functions. If implemented
// by the type passed as an argument, Type.Validate() will be called before calling the
// worker function. If an error is returned then that error will be sent in response to
// the worker request and the method will not be called.
type Validator interface {
	Validate() error
}

// Unpacker allows custom translation of request objects before calling type safe functions.
// Before calling a type safe function, Unpack() will be called on a new instance of the
// parameter type. Note that mapstructure will not be used if this interface is implemented.
type Unpacker interface {
	Unpack(interface{}) error
}

type Worker interface {
	// Stop stops the worker and waits until it is completely shutdown
	Stop()
	// Quit stops the worker asynchronously
	Quit()
	Wait()
	SetMarshalFunction(marshal MarshalFunction)
	SetUnmarshalFunction(unmarshal UnmarshalFunction)
	SetConvertTypeTagName(tagName string)
	SetConvertTypeDecoderConfig(config *DecoderConfig)
	RegisterWorkerFunction(name string, workerFunction WorkerFunction)

	// ConvertValue is a convenience method for converting a received interface{} to a specified type. It may
	// be used e.g. when a JSON request is parsed to a map[string]interface{} and you want to turn it into
	// a struct.
	ConvertValue(inputType reflect.Type, input interface{}) (output interface{}, err error)

	// MakeWorkerFunction creates a wrapper around a function that allows a function to be
	// called in a type safe manner. The function is expected to take a pointer to a struct
	// as its only argument and return one value. Internally, github.com/mitchellh/mapstructure
	// is used to convert the input parameters to a struct. MakeWorkerFunction will panic
	// if called with an incorrect type.
	MakeWorkerFunction(workerFunction interface{}) WorkerFunction

	// Start runs the worker in a new go routine
	Start() (err error)

	// Run runs the worker synchronously
	Run() (err error)
}

func ConvertValue(inputType reflect.Type, input interface{}, customUnpack bool, baseConfig *DecoderConfig, defaultTagName string) (output reflect.Value, err error) {
	output = reflect.New(inputType)
	parameters := output.Interface()
	if customUnpack {
		if err := parameters.(Unpacker).Unpack(input); err != nil {
			return reflect.ValueOf(nil), fmt.Errorf("Failed to convert parameters to type %v: %s", inputType, err)
		}
	} else {
		var config *mapstructure.DecoderConfig
		if baseConfig != nil {
			config = &mapstructure.DecoderConfig{
				Result:           parameters,
				TagName:          baseConfig.TagName,
				ErrorUnused:      baseConfig.ErrorUnused,
				ZeroFields:       baseConfig.ZeroFields,
				WeaklyTypedInput: baseConfig.WeaklyTypedInput,
				DecodeHook:       baseConfig.DecodeHook,
			}
		} else {
			config = &mapstructure.DecoderConfig{
				Metadata: nil,
				Result:   parameters,
				TagName:  defaultTagName,
			}
		}
		decoder, err := mapstructure.NewDecoder(config)
		if err != nil {
			return reflect.ValueOf(nil), fmt.Errorf("Failed to construct decoder: %s", err)
		}
		if err = decoder.Decode(input); err != nil {
			return reflect.ValueOf(nil), fmt.Errorf("Failed to convert parameters to type %v: %s", inputType, err)
		}
	}
	return
}

// MakeWorkerFunction creates a wrapper around a function that allows a function to be
// called in a type safe manner. The function is expected to take a pointer to a struct
// as its only argument and return one value. Internally, github.com/mitchellh/mapstructure
// is used to convert the input parameters to a struct. MakeWorkerFunction will panic
// if called with an incorrect type.
func MakeWorkerFunction(workerFunction interface{}, convertTypeDecoderConfig *DecoderConfig, convertTypeTagName string) WorkerFunction {
	function := reflect.ValueOf(workerFunction)
	functionType := function.Type()
	if functionType.Kind() != reflect.Func {
		log.Panicf("Attempt to convert invalid type %#v to worker function", functionType)
	}
	if functionType.IsVariadic() {
		log.Panicf("Attempt to convert variadic function %#v to worker function", function)
	}
	if functionType.NumIn() != 1 {
		log.Panicf("Worker functions must accept one and only one argument: %#v", function)
	}
	if functionType.NumOut() != 1 {
		log.Panicf("Worker functions must only return one and only one result: %#v", function)
	}
	inputType := functionType.In(0)
	if inputType.Kind() != reflect.Ptr {
		log.Panicf("Worker functions must take a pointer to a struct as their argument: %#v", function)
	}
	validate := inputType.Implements(reflect.TypeOf((*Validator)(nil)).Elem())
	customUnpack := inputType.Implements(reflect.TypeOf((*Unpacker)(nil)).Elem())
	inputType = inputType.Elem()
	if inputType.Kind() != reflect.Struct {
		log.Panicf("Worker functions must take a pointer to a struct as their argument: %#v", function)
	}
	return func(input interface{}) (output interface{}) {
		inputValue, err := ConvertValue(inputType, input, customUnpack, convertTypeDecoderConfig, convertTypeTagName)
		if err != nil {
			log.Println("Failed to decode:", err)
			return &Response{Success: false, Error: fmt.Sprintf("Failed to decode: %s", err)}
		}
		parameters := inputValue.Interface()
		if validate {
			if err := parameters.(Validator).Validate(); err != nil {
				log.Println("Validation failed:", err)
				return &Response{Success: false, Error: fmt.Sprintf("Validation failed: %s", err)}
			}
		}
		return function.Call([]reflect.Value{inputValue})[0].Interface()
	}
}
