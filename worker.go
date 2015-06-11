package gototo

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"sync/atomic"
	"time"

	zmq "github.com/JeremyOT/gototo/internal/github.com/pebbe/zmq4"
	"github.com/mitchellh/mapstructure"
)

// WorkerFunction may be registered with a worker
type WorkerFunction func(interface{}) interface{}

// MarshalFunction converts the result of a WorkerFunction to a byte slice to be
// sent back to the caller.
type MarshalFunction func(interface{}) ([]byte, error)

// UnmarshalFunction converts a byte slice to a worker invocation payload.
type UnmarshalFunction func([]byte) (*Request, error)

func unmarshalJSON(buf []byte) (data *Request, err error) {
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

// Worker listens for requests and invokes registered goroutines when called.
type Worker struct {
	logger                    *log.Logger
	registeredWorkerFunctions map[string]WorkerFunction
	activeTimeout             time.Duration
	passiveTimeout            time.Duration
	quit                      chan struct{}
	wait                      chan struct{}
	address                   string
	maxWorkers                int32
	runningWorkers            int32
	marshal                   MarshalFunction
	unmarshal                 UnmarshalFunction
	convertTypeTagName        string
	convertTypeDecoderConfig  *mapstructure.DecoderConfig
}

// Create a new worker bound to address that will run at most count functions at a time.
// If count == 0, it will default to runtime.NumCPU().
func New(address string, count int) *Worker {
	if count == 0 {
		count = runtime.NumCPU()
	}
	return &Worker{registeredWorkerFunctions: make(map[string]WorkerFunction),
		activeTimeout:      time.Millisecond,
		passiveTimeout:     100 * time.Millisecond,
		maxWorkers:         int32(count),
		address:            address,
		marshal:            json.Marshal,
		unmarshal:          unmarshalJSON,
		convertTypeTagName: "json",
	}
}

func (w *Worker) Shutdown() {
	if w.quit != nil {
		w.Quit()
		w.Wait()
	}
}

func (w *Worker) Quit() {
	defer close(w.quit)
	w.quit = nil
}

func (w *Worker) Wait() {
	<-w.wait
}

func (w *Worker) SetLogger(l *log.Logger) {
	w.logger = l
}

func (w *Worker) SetMarshalFunction(marshal MarshalFunction) {
	w.marshal = marshal
}

func (w *Worker) SetUnmarshalFunction(unmarshal UnmarshalFunction) {
	w.unmarshal = unmarshal
}

// SetConvertTypeTagName sets the tag name to use to find field information when
// converting request parameters to custom types. By default this is "json"
func (w *Worker) SetConvertTypeTagName(tagName string) {
	w.convertTypeTagName = tagName
}

// SetConvertTypeDecoderConfig sets the mapstructure config to use when decoding.
// if set, it takes precidence over SetConvertTypeTagName
func (w *Worker) SetConvertTypeDecoderConfig(config *mapstructure.DecoderConfig) {
	w.convertTypeDecoderConfig = config
}

func (w *Worker) writeLog(message ...interface{}) {
	if w.logger != nil {
		w.logger.Println(message)
	} else {
		log.Println(message)
	}
}

func (w *Worker) panicLog(message ...interface{}) {
	if w.logger != nil {
		w.logger.Panicln(message)
	} else {
		log.Panicln(message)
	}
}

func (w *Worker) RegisterWorkerFunction(name string, workerFunction WorkerFunction) {
	w.registeredWorkerFunctions[name] = workerFunction
}

func convertValue(inputType reflect.Type, input interface{}, customUnpack bool, config *mapstructure.DecoderConfig, defaultTagName string) (output reflect.Value, err error) {
	output = reflect.New(inputType)
	parameters := output.Interface()
	if customUnpack {
		if err := parameters.(Unpacker).Unpack(input); err != nil {
			return reflect.ValueOf(nil), fmt.Errorf("Failed to convert parameters to type %v: %s", inputType, err)
		}
	} else {
		var config *mapstructure.DecoderConfig
		if config != nil {
			config = &mapstructure.DecoderConfig{
				Metadata:         config.Metadata,
				Result:           parameters,
				TagName:          config.TagName,
				ErrorUnused:      config.ErrorUnused,
				ZeroFields:       config.ZeroFields,
				WeaklyTypedInput: config.WeaklyTypedInput,
				DecodeHook:       config.DecodeHook,
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

func (w *Worker) ConvertValue(inputType reflect.Type, input interface{}) (output interface{}, err error) {
	_, customUnpack := input.(Unpacker)
	outputValue, err := convertValue(inputType, input, customUnpack, w.convertTypeDecoderConfig, w.convertTypeTagName)
	if err != nil {
		return
	}
	output = outputValue.Interface()
	return
}

// MakeWorkerFunction creates a wrapper around a function that allows a function to be
// called in a type safe manner. The function is expected to take a pointer to a struct
// as its only argument and return one value. Internally, github.com/mitchellh/mapstructure
// is used to convert the input parameters to a struct. MakeWorkerFunction will panic
// if called with an incorrect type.
func (w *Worker) MakeWorkerFunction(workerFunction interface{}) WorkerFunction {
	function := reflect.ValueOf(workerFunction)
	functionType := function.Type()
	if functionType.Kind() != reflect.Func {
		w.panicLog(fmt.Sprintf("Attempt to convert invalid type %#v to worker function", functionType))
	}
	if functionType.IsVariadic() {
		w.panicLog(fmt.Sprintf("Attempt to convert variadic function %#v to worker function", function))
	}
	if functionType.NumIn() != 1 {
		w.panicLog(fmt.Sprintf("Worker functions must accept one and only one argument: %#v", function))
	}
	if functionType.NumOut() != 1 {
		w.panicLog(fmt.Sprintf("Worker functions must only return one and only one result: %#v", function))
	}
	inputType := functionType.In(0)
	if inputType.Kind() != reflect.Ptr {
		w.panicLog(fmt.Sprintf("Worker functions must take a pointer to a struct as their argument: %#v", function))
	}
	validate := inputType.Implements(reflect.TypeOf((*Validator)(nil)).Elem())
	customUnpack := inputType.Implements(reflect.TypeOf((*Unpacker)(nil)).Elem())
	inputType = inputType.Elem()
	if inputType.Kind() != reflect.Struct {
		w.panicLog(fmt.Sprintf("Worker functions must take a pointer to a struct as their argument: %#v", function))
	}
	return func(input interface{}) (output interface{}) {
		inputValue, err := convertValue(inputType, input, customUnpack, w.convertTypeDecoderConfig, w.convertTypeTagName)
		if err != nil {
			w.writeLog("Failed to decode:", err)
			return &Response{Success: false, Error: fmt.Sprintf("Failed to decode: %s", err)}
		}
		parameters := inputValue.Interface()
		if validate {
			if err := parameters.(Validator).Validate(); err != nil {
				w.writeLog("Validation failed:", err)
				return &Response{Success: false, Error: fmt.Sprintf("Validation failed: %s", err)}
			}
		}
		return function.Call([]reflect.Value{inputValue})[0].Interface()
	}
}

func (w *Worker) handlePanic(responseChannel chan [][]byte, message [][]byte) {
	if r := recover(); r != nil {
		errString := fmt.Sprintf("Panic while invoking worker function: %#v", r)
		w.writeLog(errString)
		if responseData, err := w.marshal(&Response{Success: false, Error: errString}); err != nil {
			w.writeLog("Error encoding response:", err)
		} else {
			message[len(message)-1] = responseData
			responseChannel <- message
		}
	}
}

// Run a worker function and send the response to responseChannel
func (w *Worker) runFunction(responseChannel chan [][]byte, message [][]byte, parameters interface{}, workerFunction WorkerFunction) {
	defer w.handlePanic(responseChannel, message)
	response := workerFunction(parameters)
	if responseData, err := w.marshal(response); err != nil {
		panic(err.Error())
	} else {
		message[len(message)-1] = responseData
		responseChannel <- message
	}
}

func (w *Worker) Start() (err error) {
	w.wait = make(chan struct{})
	w.quit = make(chan struct{})
	defer close(w.wait)
	socket, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		return
	}
	go w.run(socket)
	return
}

func (w *Worker) Run() {
	w.wait = make(chan struct{})
	w.quit = make(chan struct{})
	defer close(w.wait)
	socket, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		log.Println("Failed to start worker:", err)
	}
	w.run(socket)
}
func (w *Worker) run(socket *zmq.Socket) {
	defer socket.Close()
	socket.SetRcvtimeo(w.passiveTimeout)
	socket.Bind(w.address)
	atomic.StoreInt32(&w.runningWorkers, 0)
	responseChannel := make(chan [][]byte)
	sendResponse := func(response [][]byte) {
		atomic.AddInt32(&w.runningWorkers, -1)
		if _, err := socket.SendMessage(response); err != nil {
			w.writeLog("Failed to send response:", err)
		}
		if atomic.LoadInt32(&w.runningWorkers) == 0 {
			socket.SetRcvtimeo(w.passiveTimeout)
		}
	}
	for {
		if atomic.LoadInt32(&w.runningWorkers) == w.maxWorkers {
			select {
			case response := <-responseChannel:
				sendResponse(response)
			case <-w.quit:
				return
			}
		}
		select {
		case <-w.quit:
			return
		case response := <-responseChannel:
			sendResponse(response)
			break
		default:
			message, err := socket.RecvMessageBytes(0)
			if err != nil {
				// Needed to yield to goroutines when GOMAXPROCS is 1.
				// Note: The 1.3 preemptive scheduler doesn't seem to work here,
				// so this is still required.
				runtime.Gosched()
				break
			}
			request, err := w.unmarshal(message[len(message)-1])
			if err != nil {
				w.writeLog("Received invalid message", err)
				break
			}
			workerFunction := w.registeredWorkerFunctions[request.Method]
			if workerFunction == nil {
				w.writeLog("Unregistered worker function:", request.Method)
				break
			}
			if atomic.LoadInt32(&w.runningWorkers) == 0 {
				socket.SetRcvtimeo(w.activeTimeout)
			}
			atomic.AddInt32(&w.runningWorkers, 1)
			go w.runFunction(responseChannel, message, request.Parameters, workerFunction)
		}
	}
}
