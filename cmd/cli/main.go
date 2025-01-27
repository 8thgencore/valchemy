package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

// ClientConfig is the configuration for the client
type ClientConfig struct {
	Address string
}

func main() {
	config := parseFlags()

	fmt.Println("Connecting to server at", config.Address)
	conn, err := net.Dial("tcp", config.Address)
	if err != nil {
		fmt.Printf("Failed to connect to server: %v\n", err)
		return
	}

	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Printf("Failed to close connection: %v\n", err)
		}
	}()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Connected to Valchemy server. Type 'exit' to quit.")

	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "exit" {
			fmt.Println("Goodbye!")
			return
		}

		fmt.Fprintf(conn, "%s\n", input)

		response, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading response: %v\n", err)
			return
		}

		fmt.Print(response)
	}
}

func parseFlags() *ClientConfig {
	config := &ClientConfig{}
	flag.StringVar(&config.Address, "address", "127.0.0.1:3223", "Server address to connect to")
	flag.Parse()
	return config
}
