package main

import (
	"fmt"
	"os"

	"github.com/8thgencore/valchemy/internal/compute"
	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/server"
	"github.com/8thgencore/valchemy/internal/storage"
	"github.com/8thgencore/valchemy/pkg/logger"
	"github.com/8thgencore/valchemy/pkg/logger/sl"
)

func main() {
	config, err := config.NewConfig()
	if err != nil {
		fmt.Println("Failed to create config", sl.Err(err))
		os.Exit(1)
	}

	log := logger.New(config.Env)

	engine := storage.NewEngine()
	handler := compute.NewHandler(log, engine)

	srv := server.NewServer(log, &config.Network, handler)
	if err := srv.Start(); err != nil {
		log.Error("Server failed to start", sl.Err(err))
		os.Exit(1)
	}
}
