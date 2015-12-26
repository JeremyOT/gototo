// +build !no_zmq

package zmqworker

import (
	"log"
	"reflect"
	"runtime"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/JeremyOT/gototo"

	zmq "github.com/pebbe/zmq4"
)

type connectionRequest struct {
	endpoint string
	connect  bool
}

type workerRequest struct {
	Time         time.Time
	ID           uuid.UUID
	ResponseChan chan []byte
}

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
	marshal                  gototo.ClientMarshalFunction
	unmarshal                gototo.ClientUnmarshalFunction
	requestChannel           chan [][]byte
	activeTimeout            time.Duration
	passiveTimeout           time.Duration
	registeredConverters     map[string]gototo.ConverterFunction
	defaultOptions           map[string]*gototo.RequestOptions
	convertTypeTagName       string
	convertTypeDecoderConfig *gototo.DecoderConfig
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
		marshal:              gototo.ClientMarshalJSON,
		unmarshal:            gototo.ClientUnmarshalJSON,
		activeTimeout:        time.Millisecond,
		passiveTimeout:       100 * time.Millisecond,
		requests:             make(map[string]*workerRequest),
		registeredConverters: make(map[string]gototo.ConverterFunction),
		requestChannel:       make(chan [][]byte),
		defaultOptions:       make(map[string]*gototo.RequestOptions),
	}
}

// SetMarshalFunction sets the function used to marshal requests for transmission.
func (c *WorkerConnection) SetMarshalFunction(marshal gototo.ClientMarshalFunction) {
	c.marshal = marshal
}

//SetUnmarshalFunction sets the function used to unmarshal responses.
func (c *WorkerConnection) SetUnmarshalFunction(unmarshal gototo.ClientUnmarshalFunction) {
	c.unmarshal = unmarshal
}

// SetConvertTypeTagName sets the tag name to use to find field information when
// converting request parameters to custom types. By default this is "json"
func (c *WorkerConnection) SetConvertTypeTagName(tagName string) {
	c.convertTypeTagName = tagName
}

// SetConvertTypeDecoderConfig sets the config to use when decoding.
// if set, it takes precidence over SetConvertTypeTagName
func (c *WorkerConnection) SetConvertTypeDecoderConfig(config *gototo.DecoderConfig) {
	c.convertTypeDecoderConfig = config
}

// RegisterResponseType ensures that responses from calls to the named method are converted
// to the proper type before being returned to the caller. If wrappedInResponse is true then
// the response will be parsed into a Response struct before reading the result from
// Response.Result and any error from the Response will be returned as an *ResponseError from the
// ConverterFunction.
func (c *WorkerConnection) RegisterResponseType(method string, i interface{}, wrappedInResponse bool) {
	converter := gototo.CreateConverter(i, wrappedInResponse, c.convertTypeDecoderConfig, c.convertTypeTagName)
	c.converterLock.Lock()
	defer c.converterLock.Unlock()
	c.registeredConverters[method] = converter
}

// RegisterDefaultOptions sets the options that will be used whenever Call is used for a given method.
// This does not affect CallWithOptions.
func (c *WorkerConnection) RegisterDefaultOptions(method string, options *gototo.RequestOptions) {
	c.optionLock.Lock()
	defer c.optionLock.Unlock()
	c.defaultOptions[method] = options
}

// ConvertValue is a convenience method for converting a received interface{} to a specified type. It may
// be used e.g. when a JSON response is parsed to a map[string]interface{} and you want to turn it into
// a struct.
func (c *WorkerConnection) ConvertValue(inputType reflect.Type, input interface{}) (output interface{}, err error) {
	_, customUnpack := input.(gototo.Unpacker)
	outputValue, err := gototo.ConvertValue(inputType, input, customUnpack, c.convertTypeDecoderConfig, c.convertTypeTagName)
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
func (c *WorkerConnection) CallWithOptions(method string, parameters interface{}, options *gototo.RequestOptions) (response interface{}, err error) {
	data, err := c.marshal(&gototo.Request{Method: method, Parameters: parameters})
	if err != nil {
		return
	}
	if options == nil {
		options = &gototo.GlobalDefaultRequestOptions
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
				return nil, gototo.ErrTimeout
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
