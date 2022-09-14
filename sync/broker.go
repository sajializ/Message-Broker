package main

import (
	"encoding/json"
	"io/ioutil"
	"net"
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

var addresses Addresses

func main() {
	addresses = readConfig()
	status := PENDING
	link, _ := net.Listen("tcp", addresses.brokerAddress)
	defer link.Close()

	for {
		conn, _ := link.Accept()
		defer conn.Close()

		status = READING

		for status == READING {
			buffer := make([]byte, 64)
			_, err := conn.Read(buffer)
			ack := getAckMessage(buffer)
			if err != nil {
				status = PENDING
			} else {
				for _, clientAddress := range addresses.clientAddresses {
				wait_for_client:
					clientConn := tryConnect(clientAddress)
					defer clientConn.Close()
					clientConn.Write(buffer)

					_, err = clientConn.Read(make([]byte, 3))
					if err != nil {
						goto wait_for_client
					}
				}
				ackConn := tryConnect(addresses.serverAckAddress)
				defer ackConn.Close()
				ackConn.Write([]byte(ack))
			}
		}
	}
}
