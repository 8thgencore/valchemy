package wal

import (
	"fmt"
	"sync"
	"time"

	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/wal/entry"
	"github.com/8thgencore/valchemy/internal/wal/segment"
)

// Service represents the Write-Ahead Log that provides durability guarantees
// by writing entries to disk before acknowledging the write operation.
type Service struct {
	// Configuration
	config config.WALConfig

	// Segment management
	currentSegment segment.Segment

	// Batch processing
	batch   []entry.Entry
	batchMu sync.Mutex

	// Flush coordination
	flushTimer  *time.Timer
	flushDone   []chan error // Channels for notifying waiting goroutines
	timerActive bool

	// Service lifecycle
	quit chan struct{}
}

// New creates a new WAL instance with the provided configuration.
// Returns nil if WAL is disabled in the configuration.
func New(cfg config.WALConfig) (*Service, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	segment, err := segment.NewSegment(cfg.DataDirectory)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCreateSegment, err)
	}

	w := &Service{
		config:         cfg,
		batch:          make([]entry.Entry, 0, cfg.FlushingBatchSize),
		currentSegment: segment,
		quit:           make(chan struct{}),
		flushDone:      make([]chan error, 0),
	}

	return w, nil
}

// Write adds an entry to the WAL batch and ensures it's written to disk
// either immediately if the batch is full or after the flush timeout.
func (w *Service) Write(entry entry.Entry) error {
	w.batchMu.Lock()

	// Add entry to batch and create notification channel
	w.batch = append(w.batch, entry)
	done := make(chan error, 1)
	w.flushDone = append(w.flushDone, done)

	// Handle immediate flush if batch is full
	if len(w.batch) >= w.config.FlushingBatchSize {
		err := w.flushBatch()
		w.notifyWaiters(err)
		w.batchMu.Unlock()
		return err
	}

	// Start flush timer if needed
	w.startFlushTimerIfNeeded()
	w.batchMu.Unlock()

	// Wait for flush completion
	if err := <-done; err != nil {
		return fmt.Errorf("%w: %v", ErrFlushWAL, err)
	}

	return nil
}

// startFlushTimerIfNeeded starts a new flush timer if one isn't already active.
// Caller must hold batchMu lock.
func (w *Service) startFlushTimerIfNeeded() {
	if !w.timerActive {
		if w.flushTimer != nil {
			w.flushTimer.Stop()
		}
		w.flushTimer = time.NewTimer(w.config.FlushingBatchTimeout)
		w.timerActive = true

		go w.handleFlushTimer()
	}
}

// handleFlushTimer handles the flush timer expiration or service shutdown.
func (w *Service) handleFlushTimer() {
	select {
	case <-w.flushTimer.C:
		w.batchMu.Lock()
		err := w.flushBatch()
		w.notifyWaiters(err)
		w.timerActive = false
		w.batchMu.Unlock()
	case <-w.quit:
		w.notifyWaiters(nil)
	}
}

// notifyWaiters notifies all waiting goroutines about flush completion
// and clears the waiters list.
func (w *Service) notifyWaiters(err error) {
	for _, ch := range w.flushDone {
		ch <- err
	}
	// Clear the list of waiting channels
	w.flushDone = make([]chan error, 0)
}

// flushBatch writes the current batch to disk and manages segment rotation.
// Caller must hold batchMu lock.
func (w *Service) flushBatch() error {
	if len(w.batch) == 0 {
		return nil
	}

	// Stop and reset the timer when flushing
	if w.flushTimer != nil {
		w.flushTimer.Stop()
	}
	w.timerActive = false

	// Write entries and handle segment rotation
	for _, entry := range w.batch {
		if err := w.currentSegment.Write(entry); err != nil {
			return fmt.Errorf("%w: %v", ErrWriteEntry, err)
		}

		if w.currentSegment.Size() >= w.config.MaxSegmentSizeBytes {
			if err := w.rotateSegment(); err != nil {
				return fmt.Errorf("%w: %v", ErrRotateSegment, err)
			}
		}
	}

	// Ensure durability by syncing to disk
	if err := w.currentSegment.Sync(); err != nil {
		return fmt.Errorf("%w: %v", ErrSyncWAL, err)
	}

	w.batch = w.batch[:0]

	return nil
}

// rotateSegment creates a new segment and closes the current one
func (w *Service) rotateSegment() error {
	if err := w.currentSegment.Close(); err != nil {
		return fmt.Errorf("%w: %v", ErrCloseSegment, err)
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
	if w.quit == nil {
		return ErrWALClosed
	}
	close(w.quit)
	w.quit = nil // Prevent double close
	w.batchMu.Lock()
	defer w.batchMu.Unlock()

	if err := w.flushBatch(); err != nil {
		return fmt.Errorf("%w: %v", ErrFlushFinalBatch, err)
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
