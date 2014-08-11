package gototo

import (
	"encoding/json"
	zmq "github.com/JeremyOT/zmq4"
	"log"
	"runtime"
	"time"
)

type WorkerFunction func(interface{}) interface{}
type MarshalFunction func(interface{}) ([]byte, error)
type UnmarshalFunction func([]byte) (map[string]interface{}, error)

func unmarshalJson(buf []byte) (data map[string]interface{}, err error) {
	err = json.Unmarshal(buf, &data)
	return
}

type Worker struct {
	logger                    *log.Logger
	registeredWorkerFunctions map[string]WorkerFunction
	activeTimeout             time.Duration
	passiveTimeout            time.Duration
	quit                      chan int
	wait                      chan int
	address                   string
	maxWorkers                int
	runningWorkers            int
	marshal                   MarshalFunction
	unmarshal                 UnmarshalFunction
}

// Create a new worker bound to address that will run at most count functions at a time.
// If count == 0, it will default to runtime.NumCPU().
func New(address string, count int) *Worker {
	if count == 0 {
		count = runtime.NumCPU()
	}
	return &Worker{registeredWorkerFunctions: make(map[string]WorkerFunction),
		activeTimeout:  time.Millisecond,
		passiveTimeout: 100 * time.Millisecond,
		quit:           make(chan int),
		wait:           make(chan int),
		maxWorkers:     count,
		address:        address,
		marshal:        json.Marshal,
		unmarshal:      unmarshalJson,
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

func (w *Worker) writeLog(message ...interface{}) {
	if w.logger != nil {
		w.logger.Println(message)
	} else {
		log.Println(message)
	}
}

func (w *Worker) RegisterWorkerFunction(name string, workerFunction WorkerFunction) {
	w.registeredWorkerFunctions[name] = workerFunction
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
	w.runningWorkers = 0
	responseChannel := make(chan [][]byte)
	sendResponse := func(response [][]byte) {
		w.runningWorkers -= 1
		socket.SendMessage(response)
		if w.runningWorkers == 0 {
			socket.SetRcvtimeo(w.passiveTimeout)
		}
	}
	for {
		if w.runningWorkers == w.maxWorkers {
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
			if w.runningWorkers == 0 {
				socket.SetRcvtimeo(w.activeTimeout)
			}
			w.runningWorkers += 1
			go w.runFunction(responseChannel, message, data["parameters"], workerFunction)
		}
	}
}
