package main

import (
	"log"
	"os"

	"github.com/alecthomas/kong"

	"github.com/8thgencore/valchemy/internal/client"
)

// CLI flags.
var cli struct {
	Help bool   `help:"Show help."`
	Host string `default:"127.0.0.1" help:"Server host to connect to." short:"h"`
	Port string `default:"3223"      help:"Server port to connect to." short:"p"`
}

func main() {
	// Parse command line flags
	ctx := kong.Parse(&cli,
		kong.Name("valchemy"),
		kong.Description("A CLI application to connect to a server."),
		kong.UsageOnError(),
		kong.NoDefaultHelp(),
	)

	address := cli.Host + ":" + cli.Port

	// Create client
	client := client.New(address)

	// Connect to server
	err := client.Connect()
	if err != nil {
		log.Printf("Failed to connect: %v", err)
		os.Exit(1)
	}

	// Start client
	err = client.Run()
	if err != nil {
		log.Printf("Client error: %v", err)
		os.Exit(1)
	}

	ctx.Exit(0)
}
