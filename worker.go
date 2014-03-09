package gototo

import (
	"encoding/json"
	zmq "github.com/JeremyOT/gozmq"
	"log"
	"runtime"
)

// Run a ZMQ router->dealer at the specified addresses.
func RunRouter(routerAddress, dealerAddress string, routerBind, dealerBind bool) error {
	context, _ := zmq.NewContext()
	defer context.Close()
	router, _ := context.NewSocket(zmq.ROUTER)
	defer router.Close()
	if routerBind {
		router.Bind(routerAddress)
	} else {
		router.Connect(routerAddress)
	}
	dealer, _ := context.NewSocket(zmq.DEALER)
	defer dealer.Close()
	if dealerBind {
		dealer.Bind(dealerAddress)
	} else {
		dealer.Connect(dealerAddress)
	}
	return zmq.Device(zmq.QUEUE, router, dealer)
}

type WorkerFunction func(interface{}) interface{}

type Worker struct {
	logger                    *log.Logger
	registeredWorkerFunctions map[string]WorkerFunction
	activeTimeout             int
	passiveTimeout            int
	quit                      chan int
	wait                      chan int
	address                   string
	maxWorkers                int
	runningWorkers            int
}

// Create a new worker bound to address that will run at most count functions at a time.
// If count == 0, it will default to runtime.NumCPU().
func New(address string, count int) *Worker {
	if count == 0 {
		count = runtime.NumCPU()
	}
	return &Worker{registeredWorkerFunctions: make(map[string]WorkerFunction),
		activeTimeout:  1,
		passiveTimeout: 100,
		quit:           make(chan int),
		wait:           make(chan int),
		maxWorkers:     count,
		address:        address,
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
func runFunction(responseChannel chan [][]byte, message [][]byte, parameters interface{}, workerFunction WorkerFunction) {
	response := workerFunction(parameters)
	responseData, _ := json.Marshal(response)
	message[len(message)-1] = responseData
	responseChannel <- message
}

func (w *Worker) Run() {
	defer func() { close(w.wait) }()
	context, _ := zmq.NewContext()
	defer context.Close()
	socket, _ := context.NewSocket(zmq.ROUTER)
	defer socket.Close()
	socket.SetSockOptInt(zmq.RCVTIMEO, w.passiveTimeout)
	socket.Bind(w.address)
	w.runningWorkers = 0
	responseChannel := make(chan [][]byte)
	sendResponse := func(response [][]byte) {
		w.runningWorkers -= 1
		socket.SendMultipart(response, 0)
		if w.runningWorkers == 0 {
			socket.SetSockOptInt(zmq.RCVTIMEO, w.passiveTimeout)
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
			message, err := socket.RecvMultipart(0)
			if err != nil {
				// Needed to yield to goroutines when GOMAXPROCS is 1.
				// Note: The 1.2 preemptive scheduler doesn't seem to work here,
				// so this is still required.
				runtime.Gosched()
				break
			}
			data := map[string]interface{}{}
			json.Unmarshal(message[len(message)-1], &data)
			if data == nil {
				w.writeLog("Received invalid message")
				break
			}
			workerFunction := w.registeredWorkerFunctions[data["method"].(string)]
			if workerFunction == nil {
				w.writeLog("Unregistered worker function:", data["method"].(string))
				break
			}
			if w.runningWorkers == 0 {
				socket.SetSockOptInt(zmq.RCVTIMEO, w.activeTimeout)
			}
			w.runningWorkers += 1
			go runFunction(responseChannel, message, data["parameters"], workerFunction)
		}
	}
}
