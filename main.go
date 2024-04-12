package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// Represents Client request
type Command struct {
	Name   string
	Key    string
	Value  string
	Result chan<- string
}

// Represents Key Value Store
type KeyValueStore struct {
	store map[string]string
	set   chan Command
	get   chan Command
	del   chan Command
}

// New KeyValueStore represents new instance of KeyValueStore
func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		store: make(map[string]string),
		set:   make(chan Command),
		get:   make(chan Command),
		del:   make(chan Command),
	}
}

// Start starts the key-value store
func (kvs *KeyValueStore) Start() {
	for {
		select {
		case cmd := <-kvs.set:
			kvs.store[cmd.Key] = cmd.Value
			cmd.Result <- "Ok\r\n"
		case cmd := <-kvs.get:
			value, ok := kvs.store[cmd.Key]
			if !ok {
				cmd.Result <- "$-1\r\n"
			}
			cmd.Result <- fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
		case cmd := <-kvs.del:
			delete(kvs.store, cmd.Key)
			cmd.Result <- ":1\r\n"
		}
	}
}

func HandleConnection(conn net.Conn, kvs *KeyValueStore) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		// Read Command from client
		command, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading from client :", err)
			return
		}

		// Trim whitespace and newline characters
		command = strings.TrimSpace(command)

		// Split command into parts
		parts := strings.Split(command, " ")
		
		// Execute Command
		switch strings.ToUpper(parts[0]) {
		case "GET":
			key := parts[1]
			result := make(chan string)
			kvs.get <- Command{Name: "GET", Key: key, Result: result}
			response := <-result
			writer.WriteString(response)
			writer.Flush()
		case "SET":
			key := parts[1]
			value := parts[2]
			result := make(chan string)
			kvs.set <- Command{
				Name:   "SET",
				Key:    key,
				Value:  value,
				Result: result,
			}
			response := <-result
			writer.WriteString(response)
			writer.Flush()
		case "DEL":
			key := parts[1]
			result := make(chan string)
			kvs.del <- Command{
				Name:   "DEL",
				Key:    key,
				Result: result,
			}
			response := <-result
			writer.WriteString(response)
			writer.Flush()

		case "Quit":
			return
		default:
			writer.WriteString("Error : Unknown Command\r\n")
			writer.Flush()
		}
	}

}
func main() {

	// Initialize Key-Value store
	kvs := NewKeyValueStore()
	go kvs.Start()

	// Start TCP server
	s, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Error starting server", err)
		return
	}
	defer s.Close()

	fmt.Println("Server started , listening on port 6379 ")

	// Accept and Handle Connection
	for {
		conn, err := s.Accept()
		if err != nil {
			fmt.Println("Error accepting connection : ", err)
			continue
		}
		go HandleConnection(conn, kvs)
	}
}
