package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
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

func main() {
	id, _ := strconv.Atoi(os.Args[1])
	addresses = readConfig()
	link, _ := net.Listen("tcp", addresses.clientAddresses[id])
	defer link.Close()

	for i := 0; i < 5; i++ {
		conn, _ := link.Accept()
		defer conn.Close()
		buffer := make([]byte, 128)
		_, err := conn.Read(buffer)
		if err == nil {
			fmt.Println(string(buffer))
			conn.Write([]byte("OK"))
		}
		time.Sleep(4 * time.Second)
	}
}
