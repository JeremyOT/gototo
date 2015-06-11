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
	sync.RWMutex
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
// to the proper type before being returned to the caller.
func (c *WorkerConnection) RegisterResponseType(method string, i interface{}) {
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
		inputValue, err := convertValue(typ, input, customUnpack, c.convertTypeDecoderConfig, c.convertTypeTagName)
		if err != nil {
			return
		}
		output = inputValue.Interface()
		return
	}
	c.Lock()
	defer c.Unlock()
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

func (c *WorkerConnection) PendingOperations() int {
	c.RLock()
	defer c.RUnlock()
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
	c.Lock()
	request := c.requests[string(response[1])]
	c.Unlock()
	if request == nil {
		log.Println("Response received for invalid request: ", uuid.UUID(response[1]))
		return
	}
	request.ResponseChan <- response[2]
}

func (c *WorkerConnection) handleConnectionRequest(socket *zmq.Socket, request *connectionRequest) {
	c.Lock()
	defer c.Unlock()
	if request.connect {
		if err := socket.Connect(request.endpoint); err != nil {
			log.Printf("Failed to connect to endpoint %s: %s\n", request.endpoint, err)
		} else {
			c.endpoints[request.endpoint] = true
		}
	} else {
		if err := socket.Connect(request.endpoint); err != nil {
			log.Printf("Failed to disconnect from endpoint %s: %s\n", request.endpoint, err)
		} else {
			delete(c.endpoints, request.endpoint)
		}
	}
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
			c.handleConnectionRequest(socket, request)
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

func (c *WorkerConnection) Call(method string, parameters interface{}) (response interface{}, err error) {
	data, err := c.marshal(&Request{Method: method, Parameters: parameters})
	if err != nil {
		return
	}
	request := &WorkerRequest{ID: uuid.NewUUID(), Time: time.Now(), ResponseChan: make(chan []byte)}
	c.Lock()
	c.requests[string(request.ID)] = request
	c.Unlock()
	c.requestChannel <- [][]byte{[]byte{}, []byte(request.ID), data}
	responseData := <-request.ResponseChan
	response, err = c.unmarshal(responseData)
	if err != nil {
		return
	}
	c.RLock()
	converter := c.registeredConverters[method]
	c.RUnlock()
	if converter != nil {
		response, err = converter(response)
	}
	return
}
