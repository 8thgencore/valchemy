package client

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/8thgencore/valchemy/pkg/constants"
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
	fmt.Println("Connected to Valchemy server. Type 'help' or '?' for available commands.")

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
	// Send command to server
	if _, err := fmt.Fprintf(c.conn, "%s\n", command); err != nil {
		return fmt.Errorf("failed to send command: %w", err)
	}

	// Read the full response until the end marker
	var response strings.Builder
	buffer := make([]byte, 1024)
	for {
		n, err := c.conn.Read(buffer)
		if err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}

		response.Write(buffer[:n])
		if bytes.Contains(buffer[:n], []byte{0}) {
			break
		}
	}

	// Remove the end marker and print the response
	responseStr := strings.TrimSuffix(response.String(), constants.EndMarker)
	fmt.Print(responseStr)

	return nil
}
