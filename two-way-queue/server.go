package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"sync"
	"time"
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

var addresses Addresses
var wg sync.WaitGroup

func tryConnect(address string) net.Conn {
	conn, err := net.Dial("tcp", address)
	for err != nil {
		time.Sleep(1 * time.Second)
		conn, err = net.Dial("tcp", address)
	}
	return conn
}

func receiveAck(ackAddress string) {
	link, _ := net.Listen("tcp", ackAddress)
	defer link.Close()
	for {
		conn, _ := link.Accept()
		defer conn.Close()

		buffer := make([]byte, 128)
		conn.Read(buffer)
		fmt.Println("Received ack: " + string(buffer))
	}
}

func receiveFromBroker(serverAddress string) {
	defer wg.Done()
	link, _ := net.Listen("tcp", serverAddress)
	defer link.Close()

	for {
		conn, _ := link.Accept()
		defer conn.Close()
		buffer := make([]byte, 128)
		_, err := conn.Read(buffer)
		if err == nil {
			fmt.Println("Message from client: " + string(buffer))
			conn.Write([]byte("OK"))
		}
	}
}

func sendToBroker(address string, ackAddress string) {
	defer wg.Done()
	go receiveAck(ackAddress)

	sequence_number := 0
	for {
		conn := tryConnect(address)
		defer conn.Close()

		msg := "Message from server, seq_num: " + fmt.Sprint(sequence_number)
		conn.Write([]byte(msg))
		fmt.Println("Sent message: " + msg)

		time.Sleep(7 * time.Second)

		sequence_number += 1
	}
}

func main() {
	addresses = readConfig()
	wg.Add(2)
	go sendToBroker(addresses.brokerHeadAddress, addresses.serverAckAddress)
	go receiveFromBroker(addresses.serverAddress)
	wg.Wait()
}
