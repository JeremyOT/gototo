package httpworker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"reflect"
	"runtime/debug"
	"time"

	"github.com/JeremyOT/gototo"
)

// Worker listens for requests and invokes registered goroutines when called.
type Worker struct {
	registeredWorkerFunctions map[string]gototo.WorkerFunction
	quit                      chan struct{}
	wait                      chan struct{}
	net                       string
	laddr                     string
	connectedAddr             net.Addr
	runningWorkers            int32
	marshal                   gototo.MarshalFunction
	unmarshal                 gototo.UnmarshalFunction
	defaultContentType        string
	convertTypeTagName        string
	convertTypeDecoderConfig  *gototo.DecoderConfig
	marshalMap                map[string]gototo.MarshalFunction
	unmarshalMap              map[string]gototo.UnmarshalFunction
	logMetrics                bool
}

// Create a new worker bound to laddr that will run at most count functions at a time.
// If count == 0, it will default to runtime.NumCPU().
func New(laddr string) *Worker {
	return &Worker{registeredWorkerFunctions: make(map[string]gototo.WorkerFunction),
		net:                "tcp",
		laddr:              laddr,
		marshal:            json.Marshal,
		unmarshal:          gototo.UnmarshalJSON,
		marshalMap:         map[string]gototo.MarshalFunction{},
		unmarshalMap:       map[string]gototo.UnmarshalFunction{},
		convertTypeTagName: "json",
		defaultContentType: "application/json",
	}
}

func (w *Worker) SetAddress(net, laddr string) {
	w.net = net
	w.laddr = laddr
}

func (w *Worker) ConnectedAddress() net.Addr {
	return w.connectedAddr
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

func (w *Worker) SetDefaultContentType(contentType string) {
	w.defaultContentType = contentType
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

// Start runs the worker in a new go routine
func (w *Worker) Start() (err error) {
	w.wait = make(chan struct{})
	w.quit = make(chan struct{})
	listener, err := net.Listen(w.net, w.laddr)
	if err != nil {
		return
	}
	w.connectedAddr = listener.Addr()
	go w.run(listener)
	return
}

// Run runs the worker synchronously
func (w *Worker) Run() (err error) {
	w.wait = make(chan struct{})
	w.quit = make(chan struct{})
	listener, err := net.Listen(w.net, w.laddr)
	if err != nil {
		log.Println("Failed to start worker:", err)
		return
	}
	w.connectedAddr = listener.Addr()
	w.run(listener)
	return
}

func (w *Worker) writeResponse(writer http.ResponseWriter, contentType string, data interface{}, status int) {
	var marshaller gototo.MarshalFunction
	if marshaller = w.marshalMap[contentType]; marshaller == nil {
		marshaller = w.marshal
	}
	writer.Header().Set("Content-Type", contentType)
	writer.WriteHeader(status)
	body, err := marshaller(data)
	if err != nil {
		log.Println("Failed to marshal data:", err)
		return
	}
	_, err = writer.Write(body)
	if err != nil {
		log.Println("Failed to write response:", err)
	}
	return
}

func (w *Worker) handlePanic(writer http.ResponseWriter, contentType string) {
	if r := recover(); r != nil {
		errString := fmt.Sprintf("Panic while invoking worker function: %#v", r)
		log.Printf("Panic while invoking worker function: %s\n%s\n", errString, debug.Stack())
		w.writeResponse(writer, contentType, &gototo.Response{Success: false, Error: errString}, 500)
	}
}

func (w *Worker) handleRequest(writer http.ResponseWriter, request *http.Request) {
	contentType := request.Header.Get("Content-Type")
	if contentType == "" {
		contentType = w.defaultContentType
	}
	var unmarshaller gototo.UnmarshalFunction
	if unmarshaller = w.unmarshalMap[contentType]; unmarshaller == nil {
		unmarshaller = w.unmarshal
		contentType = w.defaultContentType
	}
	defer w.handlePanic(writer, contentType)
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		w.writeResponse(writer, contentType, gototo.CreateErrorResponse(fmt.Errorf("Failed to read request body: %s", err)), 500)
		return
	}

	requestData, err := unmarshaller(body)
	if err != nil {
		w.writeResponse(writer, contentType, gototo.CreateErrorResponse(fmt.Errorf("Failed to unmarshal request: %s", err)), 500)
		return
	}
	workerFunction := w.registeredWorkerFunctions[requestData.Method]
	if workerFunction == nil {
		w.writeResponse(writer, contentType, gototo.CreateErrorResponse(fmt.Errorf("No method found: %s", requestData.Method)), 404)
		return
	}
	startTime := time.Now()
	result := workerFunction(requestData.Parameters)
	if w.logMetrics {
		log.Println("Request", requestData.Method, "finished in", time.Now().Sub(startTime))
	}
	w.writeResponse(writer, contentType, result, 200)
}

func (w *Worker) run(listener net.Listener) {
	defer close(w.wait)
	defer listener.Close()
	server := &http.Server{Handler: http.HandlerFunc(w.handleRequest)}
	go server.Serve(listener)
	<-w.quit
}
