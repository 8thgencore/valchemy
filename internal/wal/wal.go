package wal

import (
	"fmt"
	"sync"
	"time"

	"github.com/8thgencore/valchemy/internal/config"
)

// WAL represents the Write-Ahead Log
type WAL struct {
	config          config.WALConfig
	batch           []Entry
	batchMu         sync.Mutex
	timer           *time.Timer
	currentSegment  *segment
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

	segment, err := newSegment(cfg.DataDirectory)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		config:          cfg,
		batch:           make([]Entry, 0, cfg.FlushingBatchSize),
		maxSegmentBytes: parseSize(cfg.MaxSegmentSize),
		currentSegment:  segment,
	}

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

	for _, entry := range w.batch {
		if err := w.currentSegment.write(entry); err != nil {
			return err
		}

		if w.currentSegment.size >= w.maxSegmentBytes {
			if err := w.rotateSegment(); err != nil {
				return err
			}
		}
	}

	if err := w.currentSegment.sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	w.batch = w.batch[:0]
	return nil
}

// rotateSegment creates a new segment and closes the current one
func (w *WAL) rotateSegment() error {
	if err := w.currentSegment.close(); err != nil {
		return fmt.Errorf("failed to close current segment: %w", err)
	}

	segment, err := newSegment(w.config.DataDirectory)
	if err != nil {
		return err
	}
	w.currentSegment = segment
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

// Close closes the WAL
func (w *WAL) Close() error {
	w.timer.Stop()
	w.batchMu.Lock()
	defer w.batchMu.Unlock()

	if err := w.flushBatch(); err != nil {
		return fmt.Errorf("failed to flush final batch: %w", err)
	}

	return w.currentSegment.close()
}

// Recover reads all WAL segments and returns entries for recovery
func (w *WAL) Recover() ([]*Entry, error) {
	var entries []*Entry

	segments, err := listSegments(w.config.DataDirectory)
	if err != nil {
		return nil, err
	}

	for _, segment := range segments {
		segmentEntries, err := readSegmentEntries(w.config.DataDirectory, segment)
		if err != nil {
			return nil, err
		}
		entries = append(entries, segmentEntries...)
	}

	return entries, nil
}
