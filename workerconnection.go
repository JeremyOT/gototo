package gototo

import (
	"encoding/json"
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

type workerRequest struct {
	Time         time.Time
	ID           uuid.UUID
	ResponseChan chan []byte
}

type connectionRequest struct {
	endpoint string
	connect  bool
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
type WorkerConnection struct {
	requestLock              sync.RWMutex
	converterLock            sync.RWMutex
	connectionLock           sync.RWMutex
	optionLock               sync.RWMutex
	quit                     chan struct{}
	wait                     chan struct{}
	endpoints                map[string]bool
	requests                 map[string]*workerRequest
	marshal                  ClientMarshalFunction
	unmarshal                ClientUnmarshalFunction
	requestChannel           chan [][]byte
	activeTimeout            time.Duration
	passiveTimeout           time.Duration
	registeredConverters     map[string]ConverterFunction
	defaultOptions           map[string]*RequestOptions
	convertTypeTagName       string
	convertTypeDecoderConfig *mapstructure.DecoderConfig
	connectionChan           chan *connectionRequest
	connectionResultChan     chan error
}

// NewConnection creates a new WorkerConnection and optionally prepares it to connect
// to one or more endpoints.
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
		requests:             make(map[string]*workerRequest),
		registeredConverters: make(map[string]ConverterFunction),
		requestChannel:       make(chan [][]byte),
		defaultOptions:       make(map[string]*RequestOptions),
	}
}

// SetMarshalFunction sets the function used to marshal requests for transmission.
func (c *WorkerConnection) SetMarshalFunction(marshal ClientMarshalFunction) {
	c.marshal = marshal
}

//SetUnmarshalFunction sets the function used to unmarshal responses.
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
// Response.Result and any error from the Response will be returned as an *ResponseError from the
// ConverterFunction.
func (c *WorkerConnection) RegisterResponseType(method string, i interface{}, wrappedInResponse bool) {
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

// RegisterDefaultOptions sets the options that will be used whenever Call is used for a given method.
// This does not affect CallWithOptions.
func (c *WorkerConnection) RegisterDefaultOptions(method string, options *RequestOptions) {
	c.optionLock.Lock()
	defer c.optionLock.Unlock()
	c.defaultOptions[method] = options
}

// ConvertValue is a convenience method for converting a received interface{} to a specified type. It may
// be used e.g. when a JSON response is parsed to a map[string]interface{} and you want to turn it into
// a struct.
func (c *WorkerConnection) ConvertValue(inputType reflect.Type, input interface{}) (output interface{}, err error) {
	_, customUnpack := input.(Unpacker)
	outputValue, err := convertValue(inputType, input, customUnpack, c.convertTypeDecoderConfig, c.convertTypeTagName)
	if err != nil {
		return
	}
	output = outputValue.Interface()
	return
}

// PendingRequestCount returns the number of requests currently awaiting responses
func (c *WorkerConnection) PendingRequestCount() int {
	c.requestLock.RLock()
	defer c.requestLock.RUnlock()
	return len(c.requests)
}

// Stop stops the worker connection and waits until it is completely shutdown
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
		log.Println("Response received for invalid request - it may have already been answered: ", uuid.UUID(response[1]))
		return
	}
	request.ResponseChan <- response[2]
}

// GetEndpoints returns all endpoints that the WorkerConnection is connected to.
func (c *WorkerConnection) GetEndpoints() (endpoints []string) {
	c.connectionLock.RLock()
	defer c.connectionLock.RUnlock()
	endpoints = make([]string, 0, len(c.endpoints))
	for e := range c.endpoints {
		endpoints = append(endpoints, e)
	}
	return
}

// Connect connects to a new endpoint
func (c *WorkerConnection) Connect(endpoint string) error {
	c.connectionChan <- &connectionRequest{endpoint: endpoint, connect: true}
	return <-c.connectionResultChan
}

// Disconnect disconnects from an existing endpoint
func (c *WorkerConnection) Disconnect(endpoint string) error {
	c.connectionChan <- &connectionRequest{endpoint: endpoint, connect: false}
	return <-c.connectionResultChan
}

// SetEndpoints connects to any new endpoints contained in the supplied list and disconnects
// from any current endpoints not in the list.
func (c *WorkerConnection) SetEndpoints(endpoint ...string) error {
	toConnect := make([]string, 0, len(endpoint))
	toDisconnect := make([]string, 0, len(endpoint))
	setEndpoints := make(map[string]bool, len(endpoint))
	c.connectionLock.RLock()
	for _, endpoint := range endpoint {
		setEndpoints[endpoint] = true
		if _, ok := c.endpoints[endpoint]; !ok {
			toConnect = append(toConnect, endpoint)
		}
	}
	for endpoint := range c.endpoints {
		if _, ok := setEndpoints[endpoint]; !ok {
			toDisconnect = append(toDisconnect, endpoint)
		}
	}
	c.connectionLock.RUnlock()
	for _, endpoint := range toConnect {
		if err := c.Connect(endpoint); err != nil {
			return err
		}
	}
	for _, endpoint := range toDisconnect {
		if err := c.Disconnect(endpoint); err != nil {
			return err
		}
	}
	return nil
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
// convert responses into the correct type. Use RegisterDefaultOptions to set a RequestOptions to use with a given
// method.
func (c *WorkerConnection) Call(method string, parameters interface{}) (response interface{}, err error) {
	c.optionLock.RLock()
	options := c.defaultOptions[method]
	c.optionLock.RUnlock()
	return c.CallWithOptions(method, parameters, options)
}

// CallWithOptions calls a method on a worker and blocks until it receives a response. Use RegisterResponseType to automatically
// convert responses into the correct type. This method is like Call but allows the caller to specify RequestOptions.
func (c *WorkerConnection) CallWithOptions(method string, parameters interface{}, options *RequestOptions) (response interface{}, err error) {
	data, err := c.marshal(&Request{Method: method, Parameters: parameters})
	if err != nil {
		return
	}
	if options == nil {
		options = &GlobalDefaultRequestOptions
	}
	request := &workerRequest{ID: uuid.NewUUID(), Time: time.Now(), ResponseChan: make(chan []byte)}
	c.requestLock.Lock()
	c.requests[string(request.ID)] = request
	c.requestLock.Unlock()
	var responseData []byte
	retriesRemaining := options.RetryCount
RetryLoop:
	for {
		var timeout <-chan time.Time
		if options.Timeout > 0 {
			timeout = time.After(options.Timeout)
		}
		c.requestChannel <- [][]byte{[]byte{}, []byte(request.ID), data}
		select {
		case responseData = <-request.ResponseChan:
			break RetryLoop
		case <-timeout:
			if retriesRemaining > 0 {
				retriesRemaining--
				continue RetryLoop
			} else {
				return nil, ErrTimeout
			}
		}
	}
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
