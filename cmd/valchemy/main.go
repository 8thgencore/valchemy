package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/8thgencore/valchemy/internal/compute"
	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/storage"
	"github.com/8thgencore/valchemy/pkg/logger"
)

func main() {
	config := config.NewConfig(config.Development)
	log := logger.New(config.Env)

	engine := storage.NewEngine()
	handler := compute.NewHandler(engine, log)

	reader := bufio.NewReader(os.Stdin)
	log.Info("Starting Valchemy. Type 'exit' to quit.")
	for {
		fmt.Print("> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		if input == "exit" {
			log.Info("Exiting Valchemy.")
			break
		}

		response, err := handler.Handle(input)
		if err != nil {
			log.Error(err.Error())
			continue
		}
		fmt.Println(response)
	}
}
