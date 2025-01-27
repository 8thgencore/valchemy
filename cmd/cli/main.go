package main

import (
	"flag"
	"log"
	"os"

	"github.com/8thgencore/valchemy/internal/client"
)

func main() {
	// Parse command line flags
	address := flag.String("address", "127.0.0.1:3223", "server address to connect to")
	flag.Parse()

	// Create client
	client := client.New(*address)

	// Connect to server
	if err := client.Connect(); err != nil {
		log.Printf("Failed to connect: %v", err)
		os.Exit(1)
	}

	// Start client
	if err := client.Run(); err != nil {
		log.Printf("Client error: %v", err)
		os.Exit(1)
	}
}
