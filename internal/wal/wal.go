package wal

import (
	"fmt"
	"sync"
	"time"

	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/wal/entry"
	"github.com/8thgencore/valchemy/internal/wal/segment"
)

// Service represents the Write-Ahead Log
type Service struct {
	config         config.WALConfig
	batch          []entry.Entry
	batchMu        sync.Mutex
	quit           chan struct{}
	currentSegment *segment.Segment
}

// Recovery represents the recovery functionality for WAL
type Recovery struct {
	Operation entry.OperationType
	Key       string
	Value     string
}

// New creates a new WAL instance
func New(cfg config.WALConfig) (*Service, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	segment, err := segment.NewSegment(cfg.DataDirectory)
	if err != nil {
		return nil, err
	}

	w := &Service{
		config:         cfg,
		batch:          make([]entry.Entry, 0, cfg.FlushingBatchSize),
		currentSegment: segment,
		quit:           make(chan struct{}),
	}

	return w, nil
}

// Write writes an operation to the WAL and ensures it's persisted before returning
func (w *Service) Write(entry entry.Entry) error {
	done := make(chan error, 1)

	w.batchMu.Lock()
	w.batch = append(w.batch, entry)

	// If the batch is full, flush it immediately
	if len(w.batch) >= w.config.FlushingBatchSize {
		err := w.flushBatch()
		w.batchMu.Unlock()
		return err
	}

	// Start a goroutine to check for the write
	go func() {
		timer := time.NewTimer(w.config.FlushingBatchTimeout)
		defer timer.Stop()

		select {
		case <-timer.C:
			w.batchMu.Lock()
			err := w.flushBatch()
			w.batchMu.Unlock()
			done <- err
		case <-w.quit:
			done <- nil
		}
	}()

	// Unlock the mutex before waiting
	w.batchMu.Unlock()

	// Wait for the write to complete or the WAL to be closed
	if err := <-done; err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	return nil
}

// flushBatch writes the current batch to disk
func (w *Service) flushBatch() error {
	if len(w.batch) == 0 {
		return nil
	}

	for _, entry := range w.batch {
		if err := w.currentSegment.Write(entry); err != nil {
			return err
		}

		if w.currentSegment.Size() >= w.config.MaxSegmentSizeBytes {
			if err := w.rotateSegment(); err != nil {
				return err
			}
		}
	}

	if err := w.currentSegment.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	w.batch = w.batch[:0]

	return nil
}

// rotateSegment creates a new segment and closes the current one
func (w *Service) rotateSegment() error {
	if err := w.currentSegment.Close(); err != nil {
		return fmt.Errorf("failed to close current segment: %w", err)
	}

	segment, err := segment.NewSegment(w.config.DataDirectory)
	if err != nil {
		return err
	}
	w.currentSegment = segment

	return nil
}

// Close closes the WAL
func (w *Service) Close() error {
	close(w.quit)
	w.batchMu.Lock()
	defer w.batchMu.Unlock()

	if err := w.flushBatch(); err != nil {
		return fmt.Errorf("failed to flush final batch: %w", err)
	}

	return w.currentSegment.Close()
}

// Recover reads all WAL segments and returns entries for recovery
func (w *Service) Recover() ([]*entry.Entry, error) {
	var entries []*entry.Entry

	segments, err := segment.ListSegments(w.config.DataDirectory)
	if err != nil {
		return nil, err
	}

	for _, s := range segments {
		segmentEntries, err := segment.ReadSegmentEntries(w.config.DataDirectory, s)
		if err != nil {
			return nil, err
		}
		entries = append(entries, segmentEntries...)
	}

	return entries, nil
}
