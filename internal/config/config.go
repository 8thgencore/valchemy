package config

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
	Env Environment
}

// NewConfig creates a new configuration for the application
func NewConfig(env Environment) *Config {
	return &Config{Env: env}
}
