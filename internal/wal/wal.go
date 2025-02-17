package wal

import (
	"fmt"
	"time"

	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/wal/entry"
	"github.com/8thgencore/valchemy/internal/wal/segment"
)

// Service represents the Write-Ahead Log that provides durability guarantees
// by writing entries to disk before acknowledging the write operation.
type Service struct {
	// Worker configuration (immutable copy)
	config struct {
		batchSize      int
		batchTimeout   time.Duration
		maxSegmentSize uint64
		dataDirectory  string
	}

	// Segment management
	currentSegment segment.Segment

	// Batch processing and lifecycle
	commands chan command
	done     chan struct{}
}

type command struct {
	entry entry.Entry
	done  chan error
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
		config: struct {
			batchSize      int
			batchTimeout   time.Duration
			maxSegmentSize uint64
			dataDirectory  string
		}{
			batchSize:      cfg.FlushingBatchSize,
			batchTimeout:   cfg.FlushingBatchTimeout,
			maxSegmentSize: cfg.MaxSegmentSizeBytes,
			dataDirectory:  cfg.DataDirectory,
		},
		currentSegment: segment,
		commands:       make(chan command),
		done:           make(chan struct{}),
	}

	go w.worker()

	return w, nil
}

func (w *Service) worker() {
	batch := make([]entry.Entry, 0, w.config.batchSize)
	timer := time.NewTimer(w.config.batchTimeout)
	defer timer.Stop()

	for {
		select {
		case cmd, ok := <-w.commands:
			if !ok {
				flushBatchIfNeeded(&batch, w, &cmd)
				close(w.done)
				return
			}
			batch = append(batch, cmd.entry)
			if len(batch) >= w.config.batchSize {
				flushBatchIfNeeded(&batch, w, &cmd)
				timer.Reset(w.config.batchTimeout)
			} else {
				cmd.done <- nil
			}
		case <-timer.C:
			flushBatchIfNeeded(&batch, w, nil)
			timer.Reset(w.config.batchTimeout)
		}
	}
}

func flushBatchIfNeeded(batch *[]entry.Entry, w *Service, cmd *command) {
	if len(*batch) > 0 {
		if err := w.flush(*batch); err != nil && cmd != nil {
			cmd.done <- err
		} else if cmd != nil {
			cmd.done <- nil
		}
		*batch = (*batch)[:0]
	}
}

// Write adds an entry to the WAL batch and ensures it's written to disk
func (w *Service) Write(entry entry.Entry) error {
	select {
	case <-w.done:
		return ErrWALClosed
	default:
	}

	done := make(chan error, 1)
	select {
	case w.commands <- command{entry: entry, done: done}:
		return <-done
	case <-w.done:
		return ErrWALClosed
	}
}

// flush writes the current batch to disk and manages segment rotation.
func (w *Service) flush(batch []entry.Entry) error {
	if len(batch) == 0 {
		return nil
	}

	// Write entries and handle segment rotation
	for _, entry := range batch {
		if err := w.currentSegment.Write(entry); err != nil {
			return fmt.Errorf("%w: %v", ErrWriteEntry, err)
		}

		if w.currentSegment.Size() >= w.config.maxSegmentSize {
			if err := w.rotateSegment(); err != nil {
				return fmt.Errorf("%w: %v", ErrRotateSegment, err)
			}
		}
	}

	// Ensure durability by syncing to disk
	if err := w.currentSegment.Sync(); err != nil {
		return fmt.Errorf("%w: %v", ErrSyncWAL, err)
	}

	return nil
}

// rotateSegment creates a new segment and closes the current one
func (w *Service) rotateSegment() error {
	if err := w.currentSegment.Close(); err != nil {
		return fmt.Errorf("%w: %v", ErrCloseSegment, err)
	}

	segment, err := segment.NewSegment(w.config.dataDirectory)
	if err != nil {
		return err
	}
	w.currentSegment = segment

	return nil
}

// Close closes the WAL
func (w *Service) Close() error {
	select {
	case <-w.done:
		return ErrWALClosed
	default:
	}

	close(w.commands)
	<-w.done

	return w.currentSegment.Close()
}

// Recover reads all WAL segments and returns entries for recovery
func (w *Service) Recover() ([]*entry.Entry, error) {
	var entries []*entry.Entry

	segments, err := segment.ListSegments(w.config.dataDirectory)
	if err != nil {
		return nil, err
	}

	for _, s := range segments {
		segmentEntries, err := segment.ReadSegmentEntries(w.config.dataDirectory, s.Name)
		if err != nil {
			return nil, err
		}
		entries = append(entries, segmentEntries...)
	}

	return entries, nil
}
