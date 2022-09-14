package main

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"sync"
	"time"
)

type Status int

const (
	READING Status = iota
	PENDING
)

type Addresses struct {
	clientAddresses  [3]string
	serverAckAddress string
	brokerAddress    string
}

func readConfig() Addresses {
	data := Addresses{}
	file, _ := ioutil.ReadFile("config.json")
	var result map[string]string
	_ = json.Unmarshal([]byte(file), &result)
	data.clientAddresses[0] = result["clientAddress1"]
	data.clientAddresses[1] = result["clientAddress2"]
	data.clientAddresses[2] = result["clientAddress3"]
	data.brokerAddress = result["brokerAddress"]
	data.serverAckAddress = result["serverAckAddress"]
	return data
}

var queue Queue
var ackChannel chan string
var wg sync.WaitGroup
var addresses Addresses

type Queue struct {
	data  [][]byte
	start int64
	end   int64
	size  int64
	mu    sync.Mutex
}

func (q *Queue) dequeue() []byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	buffer := q.data[q.start]
	q.start = (q.start + 1) % q.size
	return buffer
}

func (q *Queue) enqueue(buffer []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.data[q.end] = buffer
	q.end = (q.end + 1) % q.size
}

func (q *Queue) isEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.start == q.end {
		return true
	}
	return false
}

func tryConnect(address string) net.Conn {
	conn, err := net.Dial("tcp", address)
	for err != nil {
		time.Sleep(1 * time.Second)
		conn, err = net.Dial("tcp", address)
	}
	return conn
}

func getAckMessage(message []byte) string {
	return "Ack for: " + string(message)
}

func getOverflowMessage(message []byte) string {
	return "BROKER ERROR: OVERFLOW, IGNORING: " + string(message)
}

func manageAcks() {
	for {
		ackConn := tryConnect(addresses.serverAckAddress)
		defer ackConn.Close()

		ack := <-ackChannel
		ackConn.Write([]byte(ack))
	}
}

func receive() {
	defer wg.Done()

	link, _ := net.Listen("tcp", addresses.brokerAddress)
	defer link.Close()

	go manageAcks()

	for {
		conn, _ := link.Accept()
		defer conn.Close()

		buffer := make([]byte, 128)
		_, err := conn.Read(buffer)
		if err == nil {
			queue.enqueue(buffer)
		}
	}

}

func send() {
	defer wg.Done()
	for {
		for queue.isEmpty() {
		}

		buffer := queue.dequeue()
		for _, clientAddress := range addresses.clientAddresses {
			conn := tryConnect(clientAddress)
			defer conn.Close()

			conn.Write(buffer)
			_, err := conn.Read(make([]byte, 3))

			for err != nil {
				conn := tryConnect(clientAddress)
				conn.Write(buffer)
				_, err = conn.Read(make([]byte, 3))
			}

		}
		ackChannel <- getAckMessage(buffer)
	}
}

func main() {
	addresses = readConfig()
	queue = Queue{
		data:  make([][]byte, 1024),
		start: 0,
		end:   0,
		size:  1024,
	}
	ackChannel = make(chan string)
	wg.Add(2)

	go receive()
	go send()
	wg.Wait()
}
