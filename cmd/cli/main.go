package main

import (
	"log"
	"os"

	"github.com/8thgencore/valchemy/internal/client"
	"github.com/alecthomas/kong"
)

// CLI flags
var cli struct {
	Help bool   `help:"Show help."`
	Host string `help:"Server host to connect to." default:"127.0.0.1" short:"h"`
	Port string `help:"Server port to connect to." default:"3223" short:"p"`
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
	if err := client.Connect(); err != nil {
		log.Printf("Failed to connect: %v", err)
		os.Exit(1)
	}

	// Start client
	if err := client.Run(); err != nil {
		log.Printf("Client error: %v", err)
		os.Exit(1)
	}

	ctx.Exit(0)
}
