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
	clientAddresses    []string
	serverAddress      string
	clientAckAddresses []string
	serverAckAddress   string
	brokerHeadAddress  string
	brokerTailAddress  string
}

func readConfig() Addresses {
	data := Addresses{}
	file, _ := ioutil.ReadFile("config.json")
	var result map[string]string
	_ = json.Unmarshal([]byte(file), &result)
	data.clientAckAddresses = make([]string, 3)
	data.clientAddresses = make([]string, 3)
	data.clientAddresses[0] = result["clientAddress1"]
	data.clientAddresses[1] = result["clientAddress2"]
	data.clientAddresses[2] = result["clientAddress3"]
	data.brokerHeadAddress = result["brokerHeadAddress"]
	data.brokerTailAddress = result["brokerTailAddress"]
	data.serverAckAddress = result["serverAckAddress"]
	data.serverAddress = result["serverAddress"]
	data.clientAckAddresses[0] = result["clientAckAddress1"]
	data.clientAckAddresses[1] = result["clientAckAddress2"]
	data.clientAckAddresses[2] = result["clientAckAddress3"]
	return data
}

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

func manageAcks(ackAddress string, ackChannel chan string) {
	for {
		ackConn := tryConnect(ackAddress)
		defer ackConn.Close()
		ack := <-ackChannel
		ackConn.Write([]byte(ack))
	}
}

func receive(ackAddress string, ackChannel chan string, queue *Queue, link *net.Listener) {
	defer wg.Done()

	go manageAcks(ackAddress, ackChannel)

	for {
		conn, _ := (*link).Accept()
		defer conn.Close()

		buffer := make([]byte, 128)
		_, err := conn.Read(buffer)
		if err == nil {
			queue.enqueue(buffer)
		}
	}

}

func send(addresses []string, queue *Queue, ackChannel chan string) {
	defer wg.Done()
	for {
		for queue.isEmpty() {
		}

		buffer := queue.dequeue()

		for _, address := range addresses {
			conn := tryConnect(address)
			defer conn.Close()
			conn.Write(buffer)
			_, err := conn.Read(make([]byte, 3))

			for err != nil {
				conn := tryConnect(address)
				conn.Write(buffer)
				_, err = conn.Read(make([]byte, 3))
			}
		}
		ackChannel <- getAckMessage(buffer)
	}
}

func main() {
	addresses = readConfig()
	serverToClientQueue := Queue{
		data:  make([][]byte, 1024),
		start: 0,
		end:   0,
		size:  1024,
	}
	clientToServerQueue := Queue{
		data:  make([][]byte, 1024),
		start: 0,
		end:   0,
		size:  1024,
	}

	serverAckChannel := make(chan string)
	clientAckChanneles := make([]chan string, 3)
	clientAckChanneles[0] = make(chan string)
	clientAckChanneles[1] = make(chan string)
	clientAckChanneles[2] = make(chan string)
	serverAddressArray := make([]string, 1)
	serverAddressArray[0] = addresses.serverAddress
	wg.Add(8)

	l1, _ := net.Listen("tcp", addresses.brokerHeadAddress)
	defer l1.Close()

	go receive(addresses.serverAckAddress, serverAckChannel, &serverToClientQueue, &l1)
	go send(addresses.clientAddresses, &serverToClientQueue, serverAckChannel)

	l2, _ := net.Listen("tcp", addresses.brokerTailAddress)
	defer l2.Close()

	go receive(addresses.clientAckAddresses[0], clientAckChanneles[0], &clientToServerQueue, &l2)
	go send(serverAddressArray, &clientToServerQueue, clientAckChanneles[0])

	go receive(addresses.clientAckAddresses[1], clientAckChanneles[1], &clientToServerQueue, &l2)
	go send(serverAddressArray, &clientToServerQueue, clientAckChanneles[1])

	go receive(addresses.clientAckAddresses[2], clientAckChanneles[2], &clientToServerQueue, &l2)
	go send(serverAddressArray, &clientToServerQueue, clientAckChanneles[2])
	wg.Wait()
}
