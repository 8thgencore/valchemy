package replication

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/wal/segment"
	"github.com/8thgencore/valchemy/pkg/logger/sl"
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

// startMaster starts the master replication service
func (m *Manager) startMaster() error {
	// Start TCP server for replicas to connect on the replication port
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
				m.log.Error("Failed to accept replica connection", sl.Err(err))
				continue
			}

			go m.handleReplicaConnection(conn)
		}
	}()

	return nil
}

// handleReplicaConnection handles incoming replica connections
func (m *Manager) handleReplicaConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			m.log.Error("Failed to close connection", sl.Err(err))
		}
	}()

	m.log.Info("New replica connected", "address", conn.RemoteAddr())

	// Set read timeout
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		m.log.Error("Failed to set read deadline", sl.Err(err))
		return
	}

	for {
		// Read replica's last segment ID
		var lastSegmentID int64
		if _, err := fmt.Fscanf(conn, "%d\n", &lastSegmentID); err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Update timeout and continue
				if err := conn.SetReadDeadline(time.Now().Add(m.cfg.ReadTimeout)); err != nil {
					m.log.Error("Failed to update read deadline", sl.Err(err))
					return
				}
				continue
			}
			// If error is not timeout - replica is disconnected
			m.log.Info("Replica disconnected", "address", conn.RemoteAddr())

			return
		}

		// Reset timeout after successful read
		if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			m.log.Error("Failed to reset read deadline", sl.Err(err))
			return
		}

		// Get list of segments after lastSegmentID
		segments, err := segment.ListSegments(m.walDir)
		if err != nil {
			m.log.Error("Failed to list segments", sl.Err(err))
			return
		}

		// Send new segments to replica
		for _, seg := range segments {
			if seg.ID <= lastSegmentID {
				continue
			}

			if err := m.sendSegment(conn, seg); err != nil {
				m.log.Error("Failed to send segment", sl.Err(err), "segment_id", seg.ID)
				return
			}
		}

		// Small pause before next synchronization
		time.Sleep(m.cfg.SyncInterval)
	}
}

// sendSegment sends a WAL segment to a replica
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

// startReplica starts the replica replication service
func (m *Manager) startReplica() error {
	m.log.Info("Starting replica replication service", "master", m.cfg.MasterHost)

	go func() {
		for {
			if err := m.maintainMasterConnection(); err != nil {
				m.log.Error("Failed to maintain master connection", sl.Err(err))
				time.Sleep(m.cfg.SyncRetryDelay)
			}
		}
	}()

	return nil
}

// maintainMasterConnection establishes and maintains a connection to the master
func (m *Manager) maintainMasterConnection() error {
	if m.conn != nil {
		if err := m.conn.Close(); err != nil {
			m.log.Error("Failed to close connection", sl.Err(err))
		}
		m.conn = nil
	}

	replicationAddress := fmt.Sprintf("%s:%s", m.cfg.MasterHost, m.cfg.ReplicationPort)

	var err error
	retryCount := m.cfg.SyncRetryCount

	// Try connecting with retries
	for {
		m.conn, err = net.Dial("tcp", replicationAddress)
		if err == nil {
			m.log.Info("Connected to master", "address", replicationAddress)
			break
		}
		m.log.Error("Failed to connect to master, retrying",
			sl.Err(err),
			"retry_delay", m.cfg.SyncRetryDelay)
		if retryCount > 0 {
			retryCount--
		} else {
			return fmt.Errorf("failed to connect to master after %d retries: %w", m.cfg.SyncRetryCount, err)
		}
		time.Sleep(m.cfg.SyncRetryDelay)
	}

	return m.syncWithMaster()
}

// syncWithMaster synchronizes WAL segments with the master
func (m *Manager) syncWithMaster() error {
	if m.conn == nil {
		return errors.New("no active connection to master")
	}

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
	if _, err := fmt.Fprintf(m.conn, "%d\n", lastSegmentID); err != nil {
		return fmt.Errorf("failed to send last segment ID: %w", err)
	}

	// Receive and process new segments
	for {
		var segmentID, size int64
		if _, err := fmt.Fscanf(m.conn, "%d %d\n", &segmentID, &size); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to read segment header: %w", err)
		}

		// Read segment data
		data := make([]byte, size)
		if _, err := m.conn.Read(data); err != nil {
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
