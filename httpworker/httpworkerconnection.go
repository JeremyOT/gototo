package httpworker

import (
	"bytes"
	"errors"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JeremyOT/gototo"
)

// WorkerConnection is used to make RPCs to remote workers. If multiple workers are connected,
// requests will be round-robin load balanced between them.
type WorkerConnection struct {
	requestLock       sync.RWMutex
	converterLock     sync.RWMutex
	connectionLock    sync.RWMutex
	optionLock        sync.RWMutex
	endpoints         map[string]bool
	orderedEndpoints  []*url.URL
	nextEndpointIndex int
	// requests                 map[string]*workerRequest
	marshal     gototo.ClientMarshalFunction
	unmarshal   gototo.ClientUnmarshalFunction
	contentType string
	// requestChannel           chan [][]byte
	registeredConverters     map[string]gototo.ConverterFunction
	defaultOptions           map[string]*gototo.RequestOptions
	convertTypeTagName       string
	convertTypeDecoderConfig *gototo.DecoderConfig
	pendingRequestCount      int32
}

// NewConnection creates a new WorkerConnection and optionally prepares it to connect
// to one or more endpoints.
func NewConnection(endpoints ...string) *WorkerConnection {
	endpointMap := make(map[string]bool, len(endpoints))
	for _, endpoint := range endpoints {
		endpointMap[endpoint] = true
	}
	c := &WorkerConnection{
		endpoints:            endpointMap,
		marshal:              gototo.ClientMarshalJSON,
		unmarshal:            gototo.ClientUnmarshalJSON,
		contentType:          "application/json",
		registeredConverters: make(map[string]gototo.ConverterFunction),
		defaultOptions:       make(map[string]*gototo.RequestOptions),
	}
	c.updateEndpoints()
	return c
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

// SetConvertTypeDecoderConfig sets the mapstructure config to use when decoding.
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
	return int(atomic.LoadInt32(&c.pendingRequestCount))
}

// Must be called with c.connectionLock held
func (c *WorkerConnection) updateEndpoints() {
	count := len(c.endpoints)
	endpoints := make([]*url.URL, 0, count)
	for e := range c.endpoints {
		url, _ := url.Parse(e)
		endpoints = append(endpoints, url)
	}
	for i := 0; i < count; i++ {
		r := i + rand.Intn(count-i)
		endpoints[r], endpoints[i] = endpoints[i], endpoints[r]
	}
	c.nextEndpointIndex = 0
	c.orderedEndpoints = endpoints
}

// GetEndpoints returns all endpoints that the WorkerConnection is connected to.
func (c *WorkerConnection) GetEndpoints() (endpoints []string) {
	c.connectionLock.RLock()
	defer c.connectionLock.RUnlock()
	endpoints = make([]string, 0, len(c.endpoints))
	for e := range c.endpoints {
		endpoints = append(endpoints, e)
	}
	return endpoints
}

// Connect connects to a new endpoint
func (c *WorkerConnection) Connect(endpoint string) error {
	c.connectionLock.Lock()
	defer c.connectionLock.Unlock()
	if _, ok := c.endpoints[endpoint]; ok {
		return nil
	}
	if _, err := url.Parse(endpoint); err != nil {
		return err
	}
	c.endpoints[endpoint] = true
	c.updateEndpoints()
	return nil
}

// Disconnect disconnects from an existing endpoint
func (c *WorkerConnection) Disconnect(endpoint string) error {
	c.connectionLock.Lock()
	defer c.connectionLock.Unlock()
	if _, ok := c.endpoints[endpoint]; !ok {
		return nil
	}
	delete(c.endpoints, endpoint)
	c.updateEndpoints()
	return nil
}

// SetEndpoints connects to any new endpoints contained in the supplied list and disconnects
// from any current endpoints not in the list.
func (c *WorkerConnection) SetEndpoints(endpoint ...string) error {
	c.connectionLock.Lock()
	defer c.connectionLock.Unlock()
	newEndpoints := make(map[string]bool, len(endpoint))
	for _, endpoint := range endpoint {
		newEndpoints[endpoint] = true
	}
	c.endpoints = newEndpoints
	c.updateEndpoints()
	return nil
}

func (c *WorkerConnection) nextEndpoint() (endpoint *url.URL) {
	c.connectionLock.Lock()
	defer c.connectionLock.Unlock()
	if len(c.orderedEndpoints) == 0 {
		return nil
	}
	endpoint = c.orderedEndpoints[c.nextEndpointIndex]
	c.nextEndpointIndex++
	if c.nextEndpointIndex >= len(c.orderedEndpoints) {
		c.nextEndpointIndex = 0
	}
	return
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

func IsTimeout(err error) bool {
	if urlErr, ok := err.(*url.Error); ok {
		return IsTimeout(urlErr.Err)
	}
	if opError, ok := err.(*net.OpError); ok {
		if opError.Err != nil && opError.Err.Error() == "use of closed network connection" {
			// Bug in go < 1.5
			return true
		}
		return opError.Timeout()
	}
	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout()
	}
	return false
}

type workerResponse struct {
	response interface{}
	err      error
}

func (c *WorkerConnection) makeRequest(client *http.Client, request *http.Request, converter gototo.ConverterFunction, responseChannel chan *workerResponse) {
	resp, err := client.Do(request)
	if err != nil {
		responseChannel <- &workerResponse{err: err}
		return
	}
	defer resp.Body.Close()
	responseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		responseChannel <- &workerResponse{err: err}
		return
	}
	response, err := c.unmarshal(responseData)
	if err != nil {
		responseChannel <- &workerResponse{err: err}
		return
	}
	if converter != nil {
		response, err = converter(response)
	}
	responseChannel <- &workerResponse{response: response, err: err}
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
	retriesRemaining := options.RetryCount
	client := &http.Client{Transport: http.DefaultTransport}
	requests := make([]*http.Request, 0, retriesRemaining+1)
	responseChannel := make(chan *workerResponse)
	c.converterLock.RLock()
	converter := c.registeredConverters[method]
	c.converterLock.RUnlock()
	receivedResponseCount := 0
RetryLoop:
	for {
		nextEndpoint := c.nextEndpoint()
		if nextEndpoint == nil {
			return nil, errors.New("Attempt to call method without any active connections.")
		}
		request := &http.Request{
			Method:        "POST",
			URL:           nextEndpoint,
			Header:        http.Header{"Content-Type": []string{"application/json"}},
			Body:          ioutil.NopCloser(bytes.NewBuffer(data)),
			Close:         false,
			ContentLength: int64(len(data)),
		}
		var timeout <-chan time.Time
		if options.Timeout > 0 {
			timeout = time.After(options.Timeout)
		}
		requests = append(requests, request)
		go c.makeRequest(client, request, converter, responseChannel)
		select {
		case r := <-responseChannel:
			receivedResponseCount++
			response = r.response
			err = r.err
			break RetryLoop
		case <-timeout:
			if retriesRemaining <= 0 {
				err = gototo.ErrTimeout
				break RetryLoop
			}
			retriesRemaining--
		}
	}
	for _, req := range requests {
		http.DefaultTransport.(*http.Transport).CancelRequest(req)
	}
	if receivedResponseCount < len(requests) {
		for _ = range responseChannel {
			receivedResponseCount++
			if receivedResponseCount == len(requests) {
				break
			}
		}
	}
	return
}
