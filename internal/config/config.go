package config

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

// Environment type for environment
type Environment string

const (
	// Development is the development environment
	Development Environment = "development"
	// Production is the production environment
	Production Environment = "production"
)

// Config is the configuration for the application
type Config struct {
	Env Environment `env:"ENV" env-default:"development"`
}

// NewConfig creates a new instance of Config.
func NewConfig() (*Config, error) {
	configPath := fetchConfigPath()

	cfg := &Config{}
	var err error

	if configPath != "" {
		err = godotenv.Load(configPath)
	} else {
		err = godotenv.Load()
	}
	if err != nil {
		log.Printf("No loading .env file: %v", err)
	}

	if err = cleanenv.ReadEnv(cfg); err != nil {
		return nil, fmt.Errorf("error reading env: %w", err)
	}
	log.Printf("Load environment: %s", cfg.Env)

	return cfg, nil
}

func fetchConfigPath() string {
	var configPath string
	flag.StringVar(&configPath, "config", ".env", "Path to config file")

	flag.Parse()

	if configPath == "" {
		configPath = os.Getenv("CONFIG_PATH")
	}

	return configPath
}
