package replication

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/8thgencore/valchemy/internal/wal/segment"
	"github.com/8thgencore/valchemy/pkg/logger/sl"
)

var errTimeout = errors.New("timeout")

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

	var lastSegmentID int64 = -1
	var lastSegmentSize int64

	if err := m.conn.SetReadDeadline(time.Now().Add(m.cfg.SyncInterval)); err != nil {
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	for {
		m.log.Debug("Starting sync cycle with master")

		if err := m.updateLastSegmentInfo(&lastSegmentID, &lastSegmentSize); err != nil {
			return err
		}

		if err := m.sendSegmentInfo(lastSegmentID, lastSegmentSize); err != nil {
			return err
		}

		if err := m.receiveAndProcessSegments(&lastSegmentID, &lastSegmentSize); err != nil {
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
			if err == errTimeout || err == io.EOF {
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

func (m *Manager) readSegmentHeader() (int64, int64, error) {
	var segmentID, size int64
	if _, err := fmt.Fscanf(m.conn, "%d %d\n", &segmentID, &size); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
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
	segPath, err := validateSegmentPath(m.walDir, segName)
	if err != nil {
		return fmt.Errorf("invalid segment path: %w", err)
	}

	// Read segment data
	data := make([]byte, size)
	if _, err := m.conn.Read(data); err != nil {
		return fmt.Errorf("failed to read segment data: %w", err)
	}

	if segmentID == *lastSegmentID {
		// For an existing segment, add new data
		existingData, err := os.ReadFile(segPath) //nolint:gosec
		if err == nil {
			// Combine existing data with new data
			combinedData := make([]byte, len(existingData)+len(data))
			copy(combinedData, existingData)
			copy(combinedData[len(existingData):], data)
			data = combinedData
		}
	}

	// Write data to disk
	if err := os.WriteFile(segPath, data, 0o600); err != nil {
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

func validateSegmentPath(walDir, segName string) (string, error) {
	// Check if the segment name is valid
	if !strings.HasPrefix(segName, "wal-") || !strings.HasSuffix(segName, ".log") {
		return "", fmt.Errorf("invalid segment name format: %s", segName)
	}

	// Clean and normalize paths
	walDir = filepath.Clean(walDir)
	fullPath := filepath.Join(walDir, segName)
	fullPath = filepath.Clean(fullPath)

	// Check if the path is within the walDir
	relPath, err := filepath.Rel(walDir, fullPath)
	if err != nil {
		return "", fmt.Errorf("invalid path relationship: %w", err)
	}

	// Check if the path contains ".."
	if strings.Contains(relPath, "..") {
		return "", errors.New("path traversal detected")
	}

	return fullPath, nil
}
