package gototo

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"

	zmq "github.com/JeremyOT/gototo/internal/github.com/pebbe/zmq4"
	"github.com/mitchellh/mapstructure"
)

var responseType = reflect.TypeOf(Response{})

type ResponseError struct {
	text string
}

func (err *ResponseError) Error() string {
	return err.text
}

// ClientMarshalFunction converts a Request into a byte slice to send to worker
type ClientMarshalFunction func(req *Request) ([]byte, error)

// ClientUnmarshalFunction converts a byte slice to an interface response.
type ClientUnmarshalFunction func([]byte) (interface{}, error)

type ConverterFunction func(interface{}) (interface{}, error)

func clientMarshalJSON(req *Request) (data []byte, err error) {
	data, err = json.Marshal(req)
	return
}

func clientUnmarshalJson(buf []byte) (data interface{}, err error) {
	err = json.Unmarshal(buf, &data)
	return
}

type WorkerRequest struct {
	Time         time.Time
	ID           uuid.UUID
	ResponseChan chan []byte
}

type connectionRequest struct {
	endpoint string
	connect  bool
}

type WorkerConnection struct {
	requestLock              sync.RWMutex
	converterLock            sync.RWMutex
	connectionLock           sync.RWMutex
	quit                     chan struct{}
	wait                     chan struct{}
	endpoints                map[string]bool
	requests                 map[string]*WorkerRequest
	marshal                  ClientMarshalFunction
	unmarshal                ClientUnmarshalFunction
	requestChannel           chan [][]byte
	activeTimeout            time.Duration
	passiveTimeout           time.Duration
	registeredConverters     map[string]ConverterFunction
	convertTypeTagName       string
	convertTypeDecoderConfig *mapstructure.DecoderConfig
	connectionChan           chan *connectionRequest
	connectionResultChan     chan error
}

func NewConnection(endpoints ...string) *WorkerConnection {
	endpointMap := make(map[string]bool, len(endpoints))
	for _, endpoint := range endpoints {
		endpointMap[endpoint] = true
	}
	return &WorkerConnection{
		endpoints:            endpointMap,
		marshal:              clientMarshalJSON,
		unmarshal:            clientUnmarshalJson,
		activeTimeout:        time.Millisecond,
		passiveTimeout:       100 * time.Millisecond,
		requests:             make(map[string]*WorkerRequest),
		registeredConverters: make(map[string]ConverterFunction),
		requestChannel:       make(chan [][]byte),
	}
}

func (c *WorkerConnection) SetMarshalFunction(marshal ClientMarshalFunction) {
	c.marshal = marshal
}

func (c *WorkerConnection) SetUnmarshalFunction(unmarshal ClientUnmarshalFunction) {
	c.unmarshal = unmarshal
}

// SetConvertTypeTagName sets the tag name to use to find field information when
// converting request parameters to custom types. By default this is "json"
func (c *WorkerConnection) SetConvertTypeTagName(tagName string) {
	c.convertTypeTagName = tagName
}

// SetConvertTypeDecoderConfig sets the mapstructure config to use when decoding.
// if set, it takes precidence over SetConvertTypeTagName
func (c *WorkerConnection) SetConvertTypeDecoderConfig(config *mapstructure.DecoderConfig) {
	c.convertTypeDecoderConfig = config
}

// RegisterResponseType ensures that responses from calls to the named method are converted
// to the proper type before being returned to the caller. If wrappedInResponse is true then
// the response will be parsed into a Response struct before reading the result from
// Response.Result and any error from the Response will be retuned as an *ResponseError from the
// ConverterFunction.
func (c *WorkerConnection) RegisterResponseType(method string, i interface{}, wrappedInResponse bool) {
	typ := reflect.TypeOf(i)
	if typ.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("Only pointers to structs may be registered as a response type: %#v", typ))
	}
	customUnpack := typ.Implements(reflect.TypeOf((*Unpacker)(nil)).Elem())
	typ = typ.Elem()
	if typ.Kind() != reflect.Struct {
		panic(fmt.Sprintf("Only pointers to structs may be registered as a response type: %#v", typ))
	}
	converter := func(input interface{}) (output interface{}, err error) {
		if wrappedInResponse {
			responseValue, err := convertValue(responseType, input, false, c.convertTypeDecoderConfig, c.convertTypeTagName)
			if err != nil {
				return nil, err
			}
			parsedResponse := responseValue.Interface().(*Response)
			if !parsedResponse.Success {
				return nil, &ResponseError{text: parsedResponse.Error}
			}
			input = parsedResponse.Result
		}
		inputValue, err := convertValue(typ, input, customUnpack, c.convertTypeDecoderConfig, c.convertTypeTagName)
		if err != nil {
			return
		}
		output = inputValue.Interface()
		return
	}
	c.converterLock.Lock()
	defer c.converterLock.Unlock()
	c.registeredConverters[method] = converter
}

func (c *WorkerConnection) ConvertValue(inputType reflect.Type, input interface{}) (output interface{}, err error) {
	_, customUnpack := input.(Unpacker)
	outputValue, err := convertValue(inputType, input, customUnpack, c.convertTypeDecoderConfig, c.convertTypeTagName)
	if err != nil {
		return
	}
	output = outputValue.Interface()
	return
}

func (c *WorkerConnection) PendingRequestCount() int {
	c.requestLock.RLock()
	defer c.requestLock.RUnlock()
	return len(c.requests)
}

func (c *WorkerConnection) Stop() {
	if c.quit != nil {
		close(c.quit)
		<-c.wait
	}
}

func (c *WorkerConnection) Start() (err error) {
	c.quit = make(chan struct{})
	c.wait = make(chan struct{})
	c.connectionChan = make(chan *connectionRequest)
	c.connectionResultChan = make(chan error)
	socket, err := zmq.NewSocket(zmq.DEALER)
	if err != nil {
		return
	}
	for endpoint := range c.endpoints {
		err = socket.Connect(endpoint)
		if err != nil {
			return
		}
	}
	go c.run(socket)
	return
}

func (c *WorkerConnection) handleResponse(response [][]byte) {
	key := string(response[1])
	c.requestLock.Lock()
	request := c.requests[key]
	delete(c.requests, key)
	c.requestLock.Unlock()
	if request == nil {
		log.Println("Response received for invalid request: ", uuid.UUID(response[1]))
		return
	}
	request.ResponseChan <- response[2]
}

func (c *WorkerConnection) GetEndpoints() (endpoints []string) {
	c.connectionLock.RLock()
	defer c.connectionLock.RUnlock()
	endpoints = make([]string, 0, len(c.endpoints))
	for e := range c.endpoints {
		endpoints = append(endpoints, e)
	}
	return
}

func (c *WorkerConnection) Connect(endpoint string) error {
	c.connectionChan <- &connectionRequest{endpoint: endpoint, connect: true}
	return <-c.connectionResultChan
}

func (c *WorkerConnection) Disconnect(endpoint string) error {
	c.connectionChan <- &connectionRequest{endpoint: endpoint, connect: false}
	return <-c.connectionResultChan
}

func (c *WorkerConnection) handleConnectionRequest(socket *zmq.Socket, request *connectionRequest) {
	c.connectionLock.Lock()
	defer c.connectionLock.Unlock()
	if request.connect {
		if err := socket.Connect(request.endpoint); err != nil {
			log.Printf("Failed to connect to endpoint %s: %s\n", request.endpoint, err)
			c.connectionResultChan <- err
		} else {
			c.endpoints[request.endpoint] = true
		}
	} else {
		if err := socket.Connect(request.endpoint); err != nil {
			log.Printf("Failed to disconnect from endpoint %s: %s\n", request.endpoint, err)
			c.connectionResultChan <- err
		} else {
			delete(c.endpoints, request.endpoint)
		}
	}
	c.connectionResultChan <- nil
}

func (c *WorkerConnection) run(socket *zmq.Socket) {
	defer socket.Close()
	socket.SetRcvtimeo(c.activeTimeout)
	for {
		select {
		case <-c.quit:
			close(c.wait)
			return
		case request := <-c.requestChannel:
			if _, err := socket.SendMessage(request); err != nil {
				log.Println("Failed to send request:", err)
			}
		case request := <-c.connectionChan:
			go c.handleConnectionRequest(socket, request)
		default:
			message, err := socket.RecvMessageBytes(0)
			if err != nil {
				// Needed to yield to goroutines when GOMAXPROCS is 1.
				// Note: The 1.3 preemptive scheduler doesn't seem to work here,
				// so this is still required.
				runtime.Gosched()
				break
			}
			socket.SetRcvtimeo(c.activeTimeout)
			go c.handleResponse(message)
		}
	}
}

// Call calls a method on a worker and blocks until it receives a response. Use RegisterResponseType to automatically
// convert responses into the correct type.
func (c *WorkerConnection) Call(method string, parameters interface{}) (response interface{}, err error) {
	data, err := c.marshal(&Request{Method: method, Parameters: parameters})
	if err != nil {
		return
	}
	request := &WorkerRequest{ID: uuid.NewUUID(), Time: time.Now(), ResponseChan: make(chan []byte)}
	c.requestLock.Lock()
	c.requests[string(request.ID)] = request
	c.requestLock.Unlock()
	c.requestChannel <- [][]byte{[]byte{}, []byte(request.ID), data}
	responseData := <-request.ResponseChan
	response, err = c.unmarshal(responseData)
	if err != nil {
		return
	}
	c.converterLock.RLock()
	converter := c.registeredConverters[method]
	c.converterLock.RUnlock()
	if converter != nil {
		response, err = converter(response)
	}
	return
}
