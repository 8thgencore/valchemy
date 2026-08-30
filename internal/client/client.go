package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/8thgencore/valchemy/pkg/constants"
)

// Client represents a client for connecting to the server.
type Client struct {
	address string
	conn    net.Conn
}

// New creates a new instance of the client.
func New(address string) *Client {
	return &Client{
		address: address,
	}
}

// Connect establishes a connection to the server.
func (c *Client) Connect() error {
	var dialer net.Dialer

	conn, err := dialer.DialContext(context.Background(), "tcp", c.address)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	c.conn = conn

	return nil
}

// Run starts the interactive client mode.
func (c *Client) Run() error {
	defer func() {
		if c.conn != nil {
			if err := c.conn.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "failed to close connection: %v\n", err)
			}
		}
	}()

	reader := bufio.NewReader(os.Stdin)

	if _, err := fmt.Fprintln(os.Stdout, "Connected to Valchemy server. Type 'help' or '?' for available commands."); err != nil {
		return fmt.Errorf("failed to write to stdout: %w", err)
	}

	for {
		if _, err := fmt.Fprint(os.Stdout, "> "); err != nil {
			return fmt.Errorf("failed to write to stdout: %w", err)
		}

		input, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("failed to read input: %w", err)
		}

		input = strings.TrimSpace(input)

		if input == "exit" {
			if _, err := fmt.Fprintln(os.Stdout, "Goodbye!"); err != nil {
				return fmt.Errorf("failed to write to stdout: %w", err)
			}

			return nil
		}

		if err := c.sendCommand(input); err != nil {
			return fmt.Errorf("command error: %w", err)
		}
	}
}

// sendCommand sends a command to the server and receives a response.
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
	if _, err := fmt.Fprint(os.Stdout, responseStr); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	return nil
}
