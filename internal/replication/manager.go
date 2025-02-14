package replication

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/8thgencore/valchemy/internal/config"
)

// Manager handles replication logic for both master and replica nodes
type Manager struct {
	cfg    config.ReplicationConfig
	log    *slog.Logger
	walDir string
	conn   net.Conn
}

// New creates a new replication manager
func New(cfg config.ReplicationConfig, log *slog.Logger, walDir string) *Manager {
	return &Manager{
		cfg:    cfg,
		log:    log,
		walDir: walDir,
	}
}

// Start starts the replication manager
func (m *Manager) Start() error {
	switch m.cfg.ReplicaType {
	case config.Master:
		return m.startMaster()
	case config.Replica:
		return m.startReplica()
	default:
		return fmt.Errorf("unknown replica type: %s", m.cfg.ReplicaType)
	}
}
