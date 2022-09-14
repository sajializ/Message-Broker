package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"time"
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

var addresses Addresses

func receiveAck() {
	link, _ := net.Listen("tcp", addresses.serverAckAddress)
	defer link.Close()

	for {
		conn, _ := link.Accept()
		defer conn.Close()

		buffer := make([]byte, 128)
		conn.Read(buffer)
		fmt.Println("Received ack: " + string(buffer))
	}
}

func tryConnect(address string) net.Conn {
	conn, err := net.Dial("tcp", address)
	for err != nil {
		time.Sleep(1 * time.Second)
		conn, err = net.Dial("tcp", address)
	}
	return conn
}

func main() {
	addresses = readConfig()
	sequence_number := 0

	go receiveAck()

	for {
		conn := tryConnect(addresses.brokerAddress)
		defer conn.Close()

		msg := "Message from server, seq_num: " + fmt.Sprint(sequence_number)
		conn.Write([]byte(msg))
		fmt.Println("Sent message: " + msg)

		time.Sleep(1 * time.Second)

		sequence_number += 1
	}

}
