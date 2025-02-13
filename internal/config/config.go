package config

import (
	"fmt"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

// Env type for environment
type Env string

const (
	// Dev is the development environment
	Dev Env = "dev"
	// Prod is the production environment
	Prod Env = "prod"
)

// Config is the configuration for the application
type Config struct {
	Env         Env `env:"ENV" env-default:"dev"`
	Engine      EngineConfig
	Network     NetworkConfig
	Logging     LoggingConfig
	WAL         WALConfig
	Replication ReplicationConfig
}

// EngineConfig is the configuration for the engine
type EngineConfig struct {
	Type string `yaml:"type" env-default:"in_memory"`
}

// NetworkConfig is the configuration for the network
type NetworkConfig struct {
	Address        string        `yaml:"address" env-default:"127.0.0.1:3223"`
	MaxConnections int           `yaml:"max_connections" env-default:"100"`
	MaxMessageSize string        `yaml:"max_message_size" env-default:"4KB"`
	IdleTimeout    time.Duration `yaml:"idle_timeout" env-default:"5m"`
}

// LoggingConfig is the configuration for the logging
type LoggingConfig struct {
	Level  string `yaml:"level" env-default:"info"`
	Output string `yaml:"output" env-default:"stdout"`
	Format string `yaml:"format" env-default:"text"`
}

// WALConfig configures the Write-Ahead Logging (WAL)
type WALConfig struct {
	Enabled              bool          `yaml:"enabled" env-default:"false"`
	FlushingBatchSize    int           `yaml:"flushing_batch_size" env-default:"100"`
	FlushingBatchTimeout time.Duration `yaml:"flushing_batch_timeout" env-default:"10ms"`
	MaxSegmentSize       string        `yaml:"max_segment_size" env-default:"10MB"`
	MaxSegmentSizeBytes  uint64        `yaml:"-"` // calculated field
	DataDirectory        string        `yaml:"data_directory" env-default:"./data/wal"`
}

// ReplicationType defines the type of replication node
type ReplicationType string

const (
	// Master is the leader node that accepts writes
	Master ReplicationType = "master"
	// Replica is the follower node that replicates from master
	Replica ReplicationType = "replica"
)

// ReplicationConfig configures the replication settings
type ReplicationConfig struct {
	ReplicaType     ReplicationType `yaml:"replica_type" env-default:"master"`
	MasterHost      string          `yaml:"master_host,omitempty"`
	ReplicationPort string          `yaml:"replication_port" env-default:"3233"`
	SyncInterval    time.Duration   `yaml:"sync_interval" env-default:"1s"`
	SyncRetryDelay  time.Duration   `yaml:"sync_retry_delay" env-default:"500ms"`
	SyncRetryCount  int             `yaml:"sync_retry_count" env-default:"3"`
	ReadTimeout     time.Duration   `yaml:"read_timeout" env-default:"10s"`
}

// NewConfig creates a new instance of Config.
func NewConfig(path string) (*Config, error) {
	cfg := &Config{}

	// Load configuration from yaml file
	if err := cleanenv.ReadConfig(path, cfg); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Load environment variables
	if err := cleanenv.ReadEnv(cfg); err != nil {
		return nil, fmt.Errorf("failed to read env variables: %w", err)
	}

	// Calculate MaxSegmentSizeBytes
	maxSegmentSizeBytes, err := parseSize(cfg.WAL.MaxSegmentSize)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max segment size: %w", err)
	}
	cfg.WAL.MaxSegmentSizeBytes = maxSegmentSizeBytes

	return cfg, nil
}
