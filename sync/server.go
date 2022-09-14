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

func tryConnect(address string) net.Conn {
	conn, err := net.Dial("tcp", address)
	for err != nil {
		time.Sleep(1 * time.Second)
		conn, err = net.Dial("tcp", address)
	}
	return conn
}

var addresses Addresses

func main() {
	addresses = readConfig()
	conn := tryConnect(addresses.brokerAddress)
	defer conn.Close()
	link, _ := net.Listen("tcp", addresses.serverAckAddress)
	defer link.Close()

	sequence_number := 0

	for {
		msg := "Message from server, seq_num: " + fmt.Sprint(sequence_number)
		conn.Write([]byte(msg))
		fmt.Println("Sent message: " + msg)

		ackConn, _ := link.Accept()
		defer ackConn.Close()
		buffer := make([]byte, 64)
		ackConn.Read(buffer)
		fmt.Println("Received ack: " + string(buffer))
		time.Sleep(1 * time.Second)
		sequence_number += 1
	}

}
