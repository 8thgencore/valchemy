package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/8thgencore/valchemy/internal/wal/segment"
	"github.com/8thgencore/valchemy/pkg/logger/sl"
)

var errTimeout = errors.New("timeout")

// startReplica starts the replica replication service.
func (m *Manager) startReplica() error {
	m.log.Info("Starting replica replication service", "master", m.cfg.MasterHost)

	go func() {
		for {
			//nolint:staticcheck // maintainMasterConnection currently always returns a non-nil error by design; the nil check guards future changes.
			err := m.maintainMasterConnection()
			//nolint:staticcheck // see above.
			if err != nil {
				m.log.Error("Failed to maintain master connection", sl.Err(err))
				time.Sleep(m.cfg.SyncRetryDelay)
			}
		}
	}()

	return nil
}

// maintainMasterConnection establishes and maintains a connection to the master.
//
//nolint:staticcheck // this always returns a non-nil error by design (it blocks until the sync loop fails); documented for callers.
func (m *Manager) maintainMasterConnection() error {
	if m.conn != nil {
		err := m.conn.Close()
		if err != nil {
			m.log.Error("Failed to close connection", sl.Err(err))
		}

		m.conn = nil
	}

	replicationAddress := net.JoinHostPort(m.cfg.MasterHost, m.cfg.ReplicationPort)

	var (
		err    error
		dialer net.Dialer
	)

	retryCount := m.cfg.SyncRetryCount

	// Try connecting with retries
	for {
		m.conn, err = dialer.DialContext(context.Background(), "tcp", replicationAddress)
		if err == nil {
			m.log.Info("Connected to master", "address", replicationAddress)

			break
		}

		m.log.Error("Failed to connect to master, retrying",
			sl.Err(err),
			slog.Duration("retry_delay", m.cfg.SyncRetryDelay))

		if retryCount > 0 {
			retryCount--
		} else {
			return fmt.Errorf("failed to connect to master after %d retries: %w", m.cfg.SyncRetryCount, err)
		}

		time.Sleep(m.cfg.SyncRetryDelay)
	}

	return m.syncWithMaster()
}

// syncWithMaster synchronizes WAL segments with the master.
func (m *Manager) syncWithMaster() error {
	if m.conn == nil {
		return errors.New("no active connection to master")
	}

	var (
		lastSegmentID   int64 = -1
		lastSegmentSize int64
	)

	err := m.conn.SetReadDeadline(time.Now().Add(m.cfg.SyncInterval))
	if err != nil {
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	for {
		m.log.Debug("Starting sync cycle with master")

		err := m.updateLastSegmentInfo(&lastSegmentID, &lastSegmentSize)
		if err != nil {
			return err
		}

		err = m.sendSegmentInfo(lastSegmentID, lastSegmentSize)
		if err != nil {
			return err
		}

		err = m.receiveAndProcessSegments(&lastSegmentID, &lastSegmentSize)
		if err != nil {
			return err
		}
	}
}

func (m *Manager) updateLastSegmentInfo(lastSegmentID, lastSegmentSize *int64) error {
	segments, err := segment.ListSegments(m.walDir)
	if err != nil {
		return fmt.Errorf("failed to list local segments: %w", err)
	}

	if len(segments) > 0 {
		lastSegment := segments[len(segments)-1]
		*lastSegmentID = lastSegment.ID

		if info, err := os.Stat(filepath.Join(m.walDir, lastSegment.Name)); err == nil {
			*lastSegmentSize = info.Size()
		}
	}

	return nil
}

func (m *Manager) sendSegmentInfo(lastSegmentID, lastSegmentSize int64) error {
	m.log.Debug("Sending segment info to master",
		"last_segment_id", lastSegmentID,
		"last_segment_size", lastSegmentSize)

	if _, err := fmt.Fprintf(m.conn, "%d %d\n", lastSegmentID, lastSegmentSize); err != nil {
		return fmt.Errorf("failed to send segment info: %w", err)
	}

	return nil
}

func (m *Manager) receiveAndProcessSegments(lastSegmentID, lastSegmentSize *int64) error {
	receivedData := false

	for {
		if err := m.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return fmt.Errorf("failed to update read deadline: %w", err)
		}

		segmentID, size, err := m.readSegmentHeader()
		if err != nil {
			if errors.Is(err, errTimeout) || errors.Is(err, io.EOF) {
				break
			}

			return err
		}

		if err := m.processReceivedSegment(segmentID, size, lastSegmentID, lastSegmentSize); err != nil {
			return err
		}

		receivedData = true
	}

	if !receivedData {
		time.Sleep(m.cfg.SyncInterval)
	}

	return nil
}

func (m *Manager) readSegmentHeader() (segmentID, size int64, err error) {
	if _, err := fmt.Fscanf(m.conn, "%d %d\n", &segmentID, &size); err != nil {
		if netErr, ok := errors.AsType[net.Error](err); ok && netErr != nil {
			return 0, 0, errTimeout
		}

		if err.Error() == "EOF" {
			return 0, 0, io.EOF
		}

		return 0, 0, fmt.Errorf("failed to read segment header: %w", err)
	}

	return segmentID, size, nil
}

func (m *Manager) processReceivedSegment(segmentID, size int64, lastSegmentID, lastSegmentSize *int64) error {
	segName := fmt.Sprintf("wal-%d.log", segmentID)

	// Read segment data
	data := make([]byte, size)
	if _, err := m.conn.Read(data); err != nil {
		return fmt.Errorf("failed to read segment data: %w", err)
	}

	if segmentID == *lastSegmentID {
		// Safe reading of existing file
		existingData, err := safeReadSegment(m.walDir, segName)
		if err == nil {
			// Combine existing data with new data
			combinedData := make([]byte, len(existingData)+len(data))
			copy(combinedData, existingData)
			copy(combinedData[len(existingData):], data)
			data = combinedData
		}
	}

	fullPath := filepath.Join(m.walDir, segName)

	// Write data to disk
	err := os.WriteFile(fullPath, data, 0o600)
	if err != nil {
		return fmt.Errorf("failed to write segment file: %w", err)
	}

	m.log.Info("Received segment from master",
		"segment_id", segmentID,
		"new_data_size", size,
		"total_size", len(data),
		"is_update", segmentID == *lastSegmentID)

	*lastSegmentID = segmentID
	*lastSegmentSize = int64(len(data))

	return nil
}
