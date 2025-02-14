package replication

import (
	"fmt"
	"net"
	"time"

	"github.com/8thgencore/valchemy/internal/wal/segment"
	"github.com/8thgencore/valchemy/pkg/logger/sl"
)

// startMaster starts the master replication service
func (m *Manager) startMaster() error {
	if m.cfg.MasterHost == "" {
		m.log.Info("Master host is not set, skipping master replication service")
		return nil
	}

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
	changes := m.startWALMonitor()

	var lastSegmentID, lastSegmentSize int64 = -1, 0
	for {
		if err := m.processReplicaSync(conn, changes, &lastSegmentID, &lastSegmentSize); err != nil {
			return
		}
	}
}

func (m *Manager) startWALMonitor() chan struct{} {
	changes := make(chan struct{}, 1)
	go m.monitorWALChanges(changes)
	return changes
}

func (m *Manager) monitorWALChanges(changes chan struct{}) {
	var lastSize int64
	for {
		if size := m.getCurrentWALSize(); size > lastSize {
			lastSize = size
			select {
			case changes <- struct{}{}:
			default:
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (m *Manager) processReplicaSync(
	conn net.Conn,
	changes chan struct{},
	lastSegmentID,
	lastSegmentSize *int64,
) error {
	select {
	case <-changes:
	default:
		if err := m.readReplicaState(conn, lastSegmentID, lastSegmentSize); err != nil {
			return err
		}
	}

	return m.sendUpdatedSegments(conn, lastSegmentID, lastSegmentSize)
}

func (m *Manager) sendUpdatedSegments(conn net.Conn, lastSegmentID, lastSegmentSize *int64) error {
	segments, err := segment.ListSegments(m.walDir)
	if err != nil {
		m.log.Error("Failed to list segments", sl.Err(err))
		return nil
	}

	for _, seg := range segments {
		if err := m.processSingleSegment(conn, seg, lastSegmentID, lastSegmentSize); err != nil {
			return err
		}
	}

	return nil
}

// sendSegment sends a WAL segment to a replica
func (m *Manager) sendSegment(conn net.Conn, segInfo segment.Info, data []byte) error {
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
