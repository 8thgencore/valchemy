package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/8thgencore/valchemy/internal/config"
)

// WAL represents the Write-Ahead Log
type WAL struct {
	config          config.WALConfig
	currentLog      *os.File
	batch           []Entry
	batchMu         sync.Mutex
	batchSize       int64
	timer           *time.Timer
	maxSegmentBytes int64
}

// Recovery represents the recovery functionality for WAL
type Recovery struct {
	Operation OperationType
	Key       string
	Value     string
}

// New creates a new WAL instance
func New(cfg config.WALConfig) (*WAL, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	// Create the WAL directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDirectory, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	w := &WAL{
		config:          cfg,
		batch:           make([]Entry, 0, cfg.FlushingBatchSize),
		maxSegmentBytes: parseSize(cfg.MaxSegmentSize),
	}

	// Create a new segment
	if err := w.createNewSegment(); err != nil {
		return nil, err
	}

	// Start the timer for periodic batch writing
	w.timer = time.NewTimer(cfg.FlushingBatchTimeout)
	go w.flushOnTimeout()

	return w, nil
}

// Write writes an operation to the WAL
func (w *WAL) Write(entry Entry) error {
	w.batchMu.Lock()
	defer w.batchMu.Unlock()

	w.batch = append(w.batch, entry)

	if len(w.batch) >= w.config.FlushingBatchSize {
		return w.flushBatch()
	}

	return nil
}

// flushBatch writes the current batch to disk
func (w *WAL) flushBatch() error {
	if len(w.batch) == 0 {
		return nil
	}

	// Write all entries from the batch
	for _, entry := range w.batch {
		n, err := entry.WriteTo(w.currentLog)
		if err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}
		w.batchSize += n
	}

	// Flush data to disk
	if err := w.currentLog.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	// Check segment size
	if w.batchSize >= w.maxSegmentBytes {
		if err := w.createNewSegment(); err != nil {
			return err
		}
	}

	// Clear the batch
	w.batch = w.batch[:0]
	return nil
}

// flushOnTimeout periodically writes a batch on timeout
func (w *WAL) flushOnTimeout() {
	for range w.timer.C {
		w.batchMu.Lock()
		_ = w.flushBatch()
		w.batchMu.Unlock()
		w.timer.Reset(w.config.FlushingBatchTimeout)
	}
}

// createNewSegment creates a new WAL segment
func (w *WAL) createNewSegment() error {
	if w.currentLog != nil {
		if err := w.currentLog.Close(); err != nil {
			return fmt.Errorf("failed to close current segment: %w", err)
		}
	}

	filename := filepath.Join(
		w.config.DataDirectory,
		fmt.Sprintf("wal-%d.log", time.Now().UnixNano()),
	)

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("failed to create new segment: %w", err)
	}

	w.currentLog = file
	w.batchSize = 0

	return nil
}

// parseSize converts a human-readable size string (e.g., "10MB") to bytes
func parseSize(size string) int64 {
	var multiplier int64 = 1
	size = strings.TrimSpace(size)

	if strings.HasSuffix(size, "KB") {
		multiplier = 1024
		size = strings.TrimSuffix(size, "KB")
	} else if strings.HasSuffix(size, "MB") {
		multiplier = 1024 * 1024
		size = strings.TrimSuffix(size, "MB")
	} else if strings.HasSuffix(size, "GB") {
		multiplier = 1024 * 1024 * 1024
		size = strings.TrimSuffix(size, "GB")
	}

	value, _ := strconv.ParseInt(size, 10, 64)
	return value * multiplier
}

// ReadEntry reads a single entry from the WAL file
func ReadEntry(r io.Reader) (*Entry, error) {
	// Read operation type
	opByte := make([]byte, 1)
	if _, err := r.Read(opByte); err != nil {
		return nil, err
	}

	entry := &Entry{Operation: OperationType(opByte[0])}

	// Read key length
	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return nil, err
	}

	// Read key
	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(r, keyBytes); err != nil {
		return nil, err
	}
	entry.Key = string(keyBytes)

	// For SET operations, read value
	if entry.Operation == OperationSet {
		var valueLen uint32
		if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
			return nil, err
		}

		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(r, valueBytes); err != nil {
			return nil, err
		}
		entry.Value = string(valueBytes)
	}

	return entry, nil
}

// Recover reads all WAL segments and returns entries for recovery
func (w *WAL) Recover() ([]*Entry, error) {
	var entries []*Entry

	// Get all segment files
	files, err := os.ReadDir(w.config.DataDirectory)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	// Sort files by timestamp to process them in order
	var segments []string
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "wal-") && strings.HasSuffix(f.Name(), ".log") {
			segments = append(segments, f.Name())
		}
	}
	sort.Strings(segments)

	// Read entries from each segment
	for _, segment := range segments {
		file, err := os.Open(filepath.Join(w.config.DataDirectory, segment))
		if err != nil {
			return nil, fmt.Errorf("failed to open segment %s: %w", segment, err)
		}
		defer file.Close()

		for {
			entry, err := ReadEntry(file)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to read entry from segment %s: %w", segment, err)
			}
			entries = append(entries, entry)
		}
	}

	return entries, nil
}
