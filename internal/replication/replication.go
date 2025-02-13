package replication

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/wal/segment"
)

// Manager handles replication logic for both master and slave nodes
type Manager struct {
	cfg    config.ReplicationConfig
	log    *slog.Logger
	walDir string
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
	case config.Slave:
		return m.startSlave()
	default:
		return fmt.Errorf("unknown replica type: %s", m.cfg.ReplicaType)
	}
}

// startMaster starts the master replication service
func (m *Manager) startMaster() error {
	// Start TCP server for slaves to connect on the replication port
	replicationAddress := fmt.Sprintf("%s:%s", m.cfg.MasterHost, m.cfg.ReplicationPort)
	listener, err := net.Listen("tcp", replicationAddress)
	if err != nil {
		return fmt.Errorf("failed to start master replication listener: %w", err)
	}

	m.log.Info("Started master replication service",
		"master_host", m.cfg.MasterHost,
		"replication_port", m.cfg.ReplicationPort)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				m.log.Error("Failed to accept slave connection", "error", err)
				continue
			}

			go m.handleSlaveConnection(conn)
		}
	}()

	return nil
}

// handleSlaveConnection handles incoming slave connections
func (m *Manager) handleSlaveConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			m.log.Error("Failed to close connection", "error", err)
		}
	}()

	m.log.Info("New slave connected", "address", conn.RemoteAddr())

	// Read slave's last segment ID
	var lastSegmentID int64
	if _, err := fmt.Fscanf(conn, "%d\n", &lastSegmentID); err != nil {
		m.log.Error("Failed to read slave's last segment ID", "error", err)
		return
	}

	// Get list of segments after lastSegmentID
	segments, err := segment.ListSegments(m.walDir)
	if err != nil {
		m.log.Error("Failed to list segments", "error", err)
		return
	}

	// Send new segments to slave
	for _, seg := range segments {
		if seg.ID <= lastSegmentID {
			continue
		}

		if err := m.sendSegment(conn, seg); err != nil {
			m.log.Error("Failed to send segment", "error", err, "segment_id", seg.ID)
			return
		}
	}
}

// sendSegment sends a WAL segment to a slave
func (m *Manager) sendSegment(conn net.Conn, segInfo segment.Info) error {
	// Read segment file
	data, err := os.ReadFile(filepath.Join(m.walDir, segInfo.Name))
	if err != nil {
		return fmt.Errorf("failed to read segment file: %w", err)
	}

	// Send segment ID and size
	if _, err := fmt.Fprintf(conn, "%d %d\n", segInfo.ID, len(data)); err != nil {
		return fmt.Errorf("failed to send segment header: %w", err)
	}

	// Send segment data
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("failed to send segment data: %w", err)
	}

	return nil
}

// startSlave starts the slave replication service
func (m *Manager) startSlave() error {
	m.log.Info("Starting slave replication service", "master", m.cfg.MasterHost)

	go func() {
		for {
			if err := m.syncWithMaster(); err != nil {
				m.log.Error("Failed to sync with master", "error", err)
			}
			time.Sleep(m.cfg.SyncInterval)
		}
	}()

	return nil
}

// syncWithMaster synchronizes WAL segments with the master
func (m *Manager) syncWithMaster() error {
	// Connect to master's replication port
	replicationAddress := fmt.Sprintf("%s:%s", m.cfg.MasterHost, m.cfg.ReplicationPort)

	var conn net.Conn
	var err error

	syncRetryCount := m.cfg.SyncRetryCount

	// Try connecting with retries
	for {
		conn, err = net.Dial("tcp", replicationAddress)
		if err == nil {
			break
		}
		m.log.Error("Failed to connect to master, retrying",
			"error", err,
			"retry_delay", m.cfg.SyncRetryDelay)
		if syncRetryCount > 0 {
			syncRetryCount--
		} else {
			return fmt.Errorf("failed to connect to master after %d retries: %w", syncRetryCount, err)
		}
		time.Sleep(m.cfg.SyncRetryDelay)
	}

	defer func() {
		if err := conn.Close(); err != nil {
			m.log.Error("Failed to close connection", "error", err)
		}
	}()

	// Get last local segment ID
	segments, err := segment.ListSegments(m.walDir)
	if err != nil {
		return fmt.Errorf("failed to list local segments: %w", err)
	}

	lastSegmentID := int64(-1)
	if len(segments) > 0 {
		lastSegmentID = segments[len(segments)-1].ID
	}

	// Send last segment ID to master
	if _, err := fmt.Fprintf(conn, "%d\n", lastSegmentID); err != nil {
		return fmt.Errorf("failed to send last segment ID: %w", err)
	}

	// Receive and process new segments
	for {
		var segmentID, size int64
		if _, err := fmt.Fscanf(conn, "%d %d\n", &segmentID, &size); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to read segment header: %w", err)
		}

		// Read segment data
		data := make([]byte, size)
		if _, err := conn.Read(data); err != nil {
			return fmt.Errorf("failed to read segment data: %w", err)
		}

		// Write segment to disk
		segName := fmt.Sprintf("wal-%d.log", segmentID)
		segPath := filepath.Join(m.walDir, segName)
		if err := os.WriteFile(segPath, data, 0o600); err != nil {
			return fmt.Errorf("failed to write segment file: %w", err)
		}

		m.log.Info("Received new segment from master", "segment_id", segmentID)
	}

	return nil
}
