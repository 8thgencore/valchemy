package client

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// Client represents a client for connecting to the server
type Client struct {
	address string
	conn    net.Conn
}

// New creates a new instance of the client
func New(address string) *Client {
	return &Client{
		address: address,
	}
}

// Connect establishes a connection to the server
func (c *Client) Connect() error {
	conn, err := net.Dial("tcp", c.address)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	c.conn = conn

	return nil
}

// Run starts the interactive client mode
func (c *Client) Run() error {
	defer func() {
		if c.conn != nil {
			_ = c.conn.Close()
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
			return nil
		}

		if err := c.sendCommand(input); err != nil {
			return fmt.Errorf("command error: %w", err)
		}
	}
}

// sendCommand sends a command to the server and receives a response
func (c *Client) sendCommand(command string) error {
	if _, err := fmt.Fprintf(c.conn, "%s\n", command); err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

	response, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	fmt.Print(response)

	return nil
}
