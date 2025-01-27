package main

import (
	"flag"
	"log"
	"os"

	"github.com/8thgencore/valchemy/internal/app"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	// Create and run application
	application, err := app.New(*configPath)
	if err != nil {
		log.Printf("Failed to initialize application: %v", err)
		os.Exit(1)
	}

	// Run application
	if err := application.Run(); err != nil {
		log.Printf("Application error: %v", err)
		os.Exit(1)
	}
}
