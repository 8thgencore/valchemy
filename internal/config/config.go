package config

import (
	"fmt"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

// Env type for environment.
type Env string

const (
	// Dev is the development environment.
	Dev Env = "dev"
	// Prod is the production environment.
	Prod Env = "prod"
)

// Config is the configuration for the application.
type Config struct {
	Env         Env `env:"ENV" env-default:"dev"`
	Engine      EngineConfig
	Network     NetworkConfig
	Logging     LoggingConfig
	WAL         WALConfig
	Replication ReplicationConfig
}

// EngineConfig is the configuration for the engine.
type EngineConfig struct {
	Type string `env-default:"in_memory" yaml:"type"`
}

// NetworkConfig is the configuration for the network.
type NetworkConfig struct {
	Address        string        `env-default:"127.0.0.1:3223" yaml:"address"`
	MaxConnections int           `env-default:"100"            yaml:"max_connections"`
	MaxMessageSize string        `env-default:"4KB"            yaml:"max_message_size"`
	IdleTimeout    time.Duration `env-default:"5m"             yaml:"idle_timeout"`
}

// LoggingConfig is the configuration for the logging.
type LoggingConfig struct {
	Level  string `env-default:"info"   yaml:"level"`
	Output string `env-default:"stdout" yaml:"output"`
	Format string `env-default:"text"   yaml:"format"`
}

// WALConfig configures the Write-Ahead Logging (WAL).
type WALConfig struct {
	Enabled              bool          `env-default:"false"      yaml:"enabled"`
	FlushingBatchSize    int           `env-default:"100"        yaml:"flushing_batch_size"`
	FlushingBatchTimeout time.Duration `env-default:"10ms"       yaml:"flushing_batch_timeout"`
	MaxSegmentSize       string        `env-default:"10MB"       yaml:"max_segment_size"`
	MaxSegmentSizeBytes  uint64        `yaml:"-"` // calculated field
	DataDirectory        string        `env-default:"./data/wal" yaml:"data_directory"`
}

// ReplicationType defines the type of replication node.
type ReplicationType string

const (
	// Master is the leader node that accepts writes.
	Master ReplicationType = "master"
	// Replica is the follower node that replicates from master.
	Replica ReplicationType = "replica"
)

// ReplicationConfig configures the replication settings.
type ReplicationConfig struct {
	ReplicaType     ReplicationType `env-default:"master"         yaml:"replica_type"`
	MasterHost      string          `yaml:"master_host,omitempty"`
	ReplicationPort string          `env-default:"3233"           yaml:"replication_port"`
	SyncInterval    time.Duration   `env-default:"1s"             yaml:"sync_interval"`
	SyncRetryDelay  time.Duration   `env-default:"500ms"          yaml:"sync_retry_delay"`
	SyncRetryCount  int             `env-default:"3"              yaml:"sync_retry_count"`
	ReadTimeout     time.Duration   `env-default:"10s"            yaml:"read_timeout"`
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
