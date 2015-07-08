// +build !no_zmq

package gototo

import (
	"encoding/json"
	"log"
	"reflect"
	"time"

	"github.com/mitchellh/mapstructure"
)

var responseType = reflect.TypeOf(Response{})

type ResponseError struct {
	text    string
	timeout bool
}

// Error returns the message from the error
func (err *ResponseError) Error() string {
	return err.text
}

// Timeout returns true if the error was caused by a timeout
func (err *ResponseError) Timeout() bool {
	return err.timeout
}

// ClientMarshalFunction converts a Request into a bytgototo.e slice to send to worker
type ClientMarshalFunction func(req *Request) ([]byte, error)

// ClientUnmarshalFunction converts a byte slice to an interface response.
type ClientUnmarshalFunction func([]byte) (interface{}, error)

type ConverterFunction func(interface{}) (interface{}, error)

func ClientMarshalJSON(req *Request) (data []byte, err error) {
	data, err = json.Marshal(req)
	return
}

func ClientUnmarshalJSON(buf []byte) (data interface{}, err error) {
	err = json.Unmarshal(buf, &data)
	return
}

// RequestOptions may be used to set additional parameters when calling remote methods.
type RequestOptions struct {
	// Timeout specifies a timeout for request. It will be retried if there have been
	// less than RetryCount attempts. Otherwise ErrTimeout will be returned.
	Timeout time.Duration `json:"timeout"`
	// RetryCount specifies the maximum number of retries to attempt when a request
	// times out. Retrying does not cancel existing requests and the first response
	// will be returned to the caller.
	RetryCount int `json:"retry_count"`
}

// GlobalDefaultRequestOptions allows global defaults to be set for requests. It will be used
// when requests are invoked with Call and no method defaults are set.
var GlobalDefaultRequestOptions = RequestOptions{}

// ErrTimeout is returned whenever a request times out and has no remaining retries.
var ErrTimeout = &ResponseError{text: "Request timed out", timeout: true}

// WorkerConnection is used to make RPCs to remote workers. If multiple workers are connected,
// requests will be round-robin load balanced between them.
type WorkerConnection interface {

	// SetMarshalFunction sets the function used to marshal requests for transmission.
	SetMarshalFunction(marshal ClientMarshalFunction)

	//SetUnmarshalFunction sets the function used to unmarshal responses.

	SetUnmarshalFunction(unmarshal ClientUnmarshalFunction)

	// SetConvertTypeTagName sets the tag name to use to find field information when
	// converting request parameters to custom types. By default this is "json"
	SetConvertTypeTagName(tagName string)

	// SetConvertTypeDecoderConfig sets the mapstructure config to use when decoding.
	// if set, it takes precidence over SetConvertTypeTagName
	SetConvertTypeDecoderConfig(config *mapstructure.DecoderConfig)

	// RegisterResponseType ensures that responses from calls to the named method are converted
	// to the proper type before being returned to the caller. If wrappedInResponse is true then
	// the response will be parsed into a Response struct before reading the result from
	// Response.Result and any error from the Response will be returned as an *ResponseError from the
	// ConverterFunction.
	RegisterResponseType(method string, i interface{}, wrappedInResponse bool)

	// RegisterDefaultOptions sets the options that will be used whenever Call is used for a given method.
	// This does not affect CallWithOptions.
	RegisterDefaultOptions(method string, options *RequestOptions)

	// ConvertValue is a convenience method for converting a received interface{} to a specified type. It may
	// be used e.g. when a JSON response is parsed to a map[string]interface{} and you want to turn it into
	// a struct.
	ConvertValue(inputType reflect.Type, input interface{}) (output interface{}, err error)

	// PendingRequestCount returns the number of requests currently awaiting responses
	PendingRequestCount() int

	// GetEndpoints returns all endpoints that the WorkerConnection is connected to.
	GetEndpoints() (endpoints []string)

	// Connect connects to a new endpoint
	Connect(endpoint string) error

	// Disconnect disconnects from an existing endpoint
	Disconnect(endpoint string) error

	// SetEndpoints connects to any new endpoints contained in the supplied list and disconnects
	// from any current endpoints not in the list.
	SetEndpoints(endpoint ...string) error

	// Call calls a method on a worker and blocks until it receives a response. Use RegisterResponseType to automatically
	// convert responses into the correct type. Use RegisterDefaultOptions to set a RequestOptions to use with a given
	// method.
	Call(method string, parameters interface{}) (response interface{}, err error)

	// CallWithOptions calls a method on a worker and blocks until it receives a response. Use RegisterResponseType to automatically
	// convert responses into the correct type. This method is like Call but allows the caller to specify RequestOptions.
	CallWithOptions(method string, parameters interface{}, options *RequestOptions) (response interface{}, err error)
}

func CreateConverter(i interface{}, wrappedInResponse bool, convertTypeDecoderConfig *mapstructure.DecoderConfig, convertTypeTagName string) ConverterFunction {
	typ := reflect.TypeOf(i)
	if typ.Kind() != reflect.Ptr {
		log.Panicf("Only pointers to structs may be registered as a response type: %#v", typ)
	}
	customUnpack := typ.Implements(reflect.TypeOf((*Unpacker)(nil)).Elem())
	typ = typ.Elem()
	if typ.Kind() != reflect.Struct {
		log.Panicf("Only pointers to structs may be registered as a response type: %#v", typ)
	}
	converter := func(input interface{}) (output interface{}, err error) {
		if wrappedInResponse {
			responseValue, err := ConvertValue(responseType, input, false, convertTypeDecoderConfig, convertTypeTagName)
			if err != nil {
				return nil, err
			}
			parsedResponse := responseValue.Interface().(*Response)
			if !parsedResponse.Success {
				return nil, &ResponseError{text: parsedResponse.Error}
			}
			input = parsedResponse.Result
		}
		inputValue, err := ConvertValue(typ, input, customUnpack, convertTypeDecoderConfig, convertTypeTagName)
		if err != nil {
			return
		}
		output = inputValue.Interface()
		return
	}
	return converter
}
