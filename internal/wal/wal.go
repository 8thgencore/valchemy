package wal

import (
	"fmt"
	"os"
	"path/filepath"
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

// New creates a new WAL instance
func New(cfg config.WALConfig) (*WAL, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	// Create the WAL directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDirectory, 0755); err != nil {
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

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
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
