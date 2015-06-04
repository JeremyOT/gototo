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
type UnmarshalFunction func([]byte) (map[string]interface{}, error)

func unmarshalJSON(buf []byte) (data map[string]interface{}, err error) {
	err = json.Unmarshal(buf, &data)
	return
}

type Response struct {
	Success bool        `json:"success"`
	Error   string      `json:"error,omitempty"`
	Result  interface{} `json:"result,omitempty"`
}

// Worker listens for requests and invokes registered goroutines when called.
type Worker struct {
	logger                    *log.Logger
	registeredWorkerFunctions map[string]WorkerFunction
	activeTimeout             time.Duration
	passiveTimeout            time.Duration
	quit                      chan int
	wait                      chan int
	address                   string
	maxWorkers                int32
	runningWorkers            int32
	marshal                   MarshalFunction
	unmarshal                 UnmarshalFunction
	convertTypeTagName        string
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
		quit:               make(chan int),
		wait:               make(chan int),
		maxWorkers:         int32(count),
		address:            address,
		marshal:            json.Marshal,
		unmarshal:          unmarshalJSON,
		convertTypeTagName: "json",
	}
}

func (w *Worker) Quit() {
	w.quit <- 1
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
	inputType = inputType.Elem()
	if inputType.Kind() != reflect.Struct {
		w.panicLog(fmt.Sprintf("Worker functions must take a pointer to a struct as their argument: %#v", function))
	}
	return func(input interface{}) (output interface{}) {
		inputValue := reflect.New(inputType)
		parameters := inputValue.Interface()
		config := &mapstructure.DecoderConfig{
			Metadata: nil,
			Result:   parameters,
			TagName:  w.convertTypeTagName,
		}
		decoder, err := mapstructure.NewDecoder(config)
		if err != nil {
			w.writeLog("Failed to construct decoder:", err)
			return &Response{Success: false, Error: fmt.Sprintf("Failed to construct decoder: %s", err)}
		}
		err = decoder.Decode(input)
		if err != nil {
			w.writeLog(fmt.Sprintf("Failed to convert parameters to type %v: %s", inputType, err))
			return &Response{Success: false, Error: fmt.Sprintf("Failed to convert parameters to type %v: %s", inputType, err)}
		}
		return function.Call([]reflect.Value{inputValue})[0].Interface()
	}
}

// Run a worker function and send the response to responseChannel
func (w *Worker) runFunction(responseChannel chan [][]byte, message [][]byte, parameters interface{}, workerFunction WorkerFunction) {
	response := workerFunction(parameters)
	responseData, _ := w.marshal(response)
	message[len(message)-1] = responseData
	responseChannel <- message
}

func (w *Worker) Run() {
	defer func() { close(w.wait) }()
	socket, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		log.Println("Failed to start worker:", err)
	}
	defer socket.Close()
	socket.SetRcvtimeo(w.passiveTimeout)
	socket.Bind(w.address)
	atomic.StoreInt32(&w.runningWorkers, 0)
	responseChannel := make(chan [][]byte)
	sendResponse := func(response [][]byte) {
		atomic.AddInt32(&w.runningWorkers, -1)
		socket.SendMessage(response)
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
			data, err := w.unmarshal(message[len(message)-1])
			if err != nil {
				w.writeLog("Received invalid message", err)
				break
			}
			workerFunction := w.registeredWorkerFunctions[data["method"].(string)]
			if workerFunction == nil {
				w.writeLog("Unregistered worker function:", data["method"].(string))
				break
			}
			if atomic.LoadInt32(&w.runningWorkers) == 0 {
				socket.SetRcvtimeo(w.activeTimeout)
			}
			atomic.AddInt32(&w.runningWorkers, 1)
			go w.runFunction(responseChannel, message, data["parameters"], workerFunction)
		}
	}
}
