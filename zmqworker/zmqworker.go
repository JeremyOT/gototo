// +build !no_zmq

package zmqworker

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/JeremyOT/gototo"
	zmq "github.com/pebbe/zmq4"
)

// Worker listens for requests and invokes registered goroutines when called.
type Worker struct {
	registeredWorkerFunctions map[string]gototo.WorkerFunction
	activeTimeout             time.Duration
	passiveTimeout            time.Duration
	quit                      chan struct{}
	wait                      chan struct{}
	address                   string
	maxWorkers                int32
	runningWorkers            int32
	marshal                   gototo.MarshalFunction
	unmarshal                 gototo.UnmarshalFunction
	convertTypeTagName        string
	convertTypeDecoderConfig  *gototo.DecoderConfig
	logMetrics                bool
}

// New creates a new worker bound to address that will run at most count functions at a time.
// If count == 0, it will default to runtime.NumCPU().
func New(address string, count int) *Worker {
	if count == 0 {
		count = runtime.NumCPU()
	}
	return &Worker{registeredWorkerFunctions: make(map[string]gototo.WorkerFunction),
		activeTimeout:      time.Millisecond,
		passiveTimeout:     100 * time.Millisecond,
		maxWorkers:         int32(count),
		address:            address,
		marshal:            json.Marshal,
		unmarshal:          gototo.UnmarshalJSON,
		convertTypeTagName: "json",
	}
}

// Stop stops the worker and waits until it is completely shutdown
func (w *Worker) Stop() {
	if w.quit != nil {
		w.Quit()
		w.Wait()
	}
}

// Quit stops the worker
func (w *Worker) Quit() {
	close(w.quit)
}

// Wait waits for the worker to completely shutdown
func (w *Worker) Wait() {
	<-w.wait
}

func (w *Worker) SetMarshalFunction(marshal gototo.MarshalFunction) {
	w.marshal = marshal
}

func (w *Worker) SetUnmarshalFunction(unmarshal gototo.UnmarshalFunction) {
	w.unmarshal = unmarshal
}

// SetConvertTypeTagName sets the tag name to use to find field information when
// converting request parameters to custom types. By default this is "json"
func (w *Worker) SetConvertTypeTagName(tagName string) {
	w.convertTypeTagName = tagName
}

// SetConvertTypeDecoderConfig sets the mapstructure config to use when decoding.
// if set, it takes precidence over SetConvertTypeTagName
func (w *Worker) SetConvertTypeDecoderConfig(config *gototo.DecoderConfig) {
	w.convertTypeDecoderConfig = config
}

// SetLogMetrics enables or disables metric logging
func (w *Worker) SetLogMetrics(logMetrics bool) {
	w.logMetrics = logMetrics
}

// RegisterWorkerFunction adds a new worker function that will be invoked when requests
// are received with the named method.
func (w *Worker) RegisterWorkerFunction(name string, workerFunction gototo.WorkerFunction) {
	w.registeredWorkerFunctions[name] = workerFunction
}

// ConvertValue is a convenience method for converting a received interface{} to a specified type. It may
// be used e.g. when a JSON request is parsed to a map[string]interface{} and you want to turn it into
// a struct.
func (w *Worker) ConvertValue(inputType reflect.Type, input interface{}) (output interface{}, err error) {
	_, customUnpack := input.(gototo.Unpacker)
	outputValue, err := gototo.ConvertValue(inputType, input, customUnpack, w.convertTypeDecoderConfig, w.convertTypeTagName)
	if err != nil {
		return
	}
	output = outputValue.Interface()
	return
}

func (w *Worker) MakeWorkerFunction(workerFunction interface{}) gototo.WorkerFunction {
	return gototo.MakeWorkerFunction(workerFunction, w.convertTypeDecoderConfig, w.convertTypeTagName)
}

func (w *Worker) handlePanic(responseChannel chan *messageContext, message *messageContext) {
	if r := recover(); r != nil {
		errString := fmt.Sprintf("Panic while invoking worker function: %#v", r)
		log.Printf("Panic while invoking worker function: %s\n%s\n", errString, debug.Stack())
		if responseData, err := w.marshal(&gototo.Response{Success: false, Error: errString}); err != nil {
			log.Println("Error encoding response:", err)
		} else {
			message.data[len(message.data)-1] = responseData
			responseChannel <- message
		}
	}
}

// Run a worker function and send the response to responseChannel
func (w *Worker) runFunction(responseChannel chan *messageContext, message *messageContext, parameters interface{}, workerFunction gototo.WorkerFunction) {
	defer w.handlePanic(responseChannel, message)
	response := workerFunction(parameters)
	if responseData, err := w.marshal(response); err != nil {
		panic(err.Error())
	} else {
		message.data[len(message.data)-1] = responseData
		responseChannel <- message
	}
}

// Start runs the worker in a new go routine
func (w *Worker) Start() (err error) {
	w.wait = make(chan struct{})
	w.quit = make(chan struct{})
	socket, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		return
	}
	go w.run(socket)
	return
}

// Run runs the worker synchronously
func (w *Worker) Run() (err error) {
	w.wait = make(chan struct{})
	w.quit = make(chan struct{})
	socket, err := zmq.NewSocket(zmq.ROUTER)
	if err != nil {
		log.Println("Failed to start worker:", err)
		return
	}
	w.run(socket)
	return
}

type messageContext struct {
	data      [][]byte
	startTime time.Time
	method    string
}

func (w *Worker) logFinish(context *messageContext) {
	if !w.logMetrics {
		return
	}
	log.Println("Request", context.method, "finished in", time.Now().Sub(context.startTime))
}

func (w *Worker) run(socket *zmq.Socket) {
	defer close(w.wait)
	defer socket.Close()
	socket.SetRcvtimeo(w.passiveTimeout)
	socket.Bind(w.address)
	atomic.StoreInt32(&w.runningWorkers, 0)
	responseChannel := make(chan *messageContext)
	sendResponse := func(response [][]byte) {
		atomic.AddInt32(&w.runningWorkers, -1)
		if _, err := socket.SendMessage(response); err != nil {
			log.Println("Failed to send response:", err)
		}
		if atomic.LoadInt32(&w.runningWorkers) == 0 {
			socket.SetRcvtimeo(w.passiveTimeout)
		}
	}
	for {
		if atomic.LoadInt32(&w.runningWorkers) == w.maxWorkers {
			// We're already running maxWorkers so block until a response is ready
			select {
			case response := <-responseChannel:
				w.logFinish(response)
				sendResponse(response.data)
			case <-w.quit:
				return
			}
		}
		select {
		case <-w.quit:
			return
		case response := <-responseChannel:
			w.logFinish(response)
			sendResponse(response.data)
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
				log.Println("Received invalid message", err)
				break
			}
			workerFunction := w.registeredWorkerFunctions[request.Method]
			if workerFunction == nil {
				log.Println("Unregistered worker function:", request.Method)
				break
			}
			context := messageContext{data: message, startTime: time.Now(), method: request.Method}
			if atomic.LoadInt32(&w.runningWorkers) == 0 {
				socket.SetRcvtimeo(w.activeTimeout)
			}
			atomic.AddInt32(&w.runningWorkers, 1)
			go w.runFunction(responseChannel, &context, request.Parameters, workerFunction)
		}
	}
}
