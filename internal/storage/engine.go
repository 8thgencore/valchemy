package storage

import (
	"log/slog"
	"sync"

	"github.com/8thgencore/valchemy/internal/wal"
	"github.com/8thgencore/valchemy/internal/wal/entry"
)

// Engine is a struct that represents the storage engine
type Engine struct {
	data map[string]string
	mu   sync.RWMutex
	wal  wal.WAL
}

// NewEngine creates a new Engine
func NewEngine(log *slog.Logger, w wal.WAL) *Engine {
	e := &Engine{
		data: make(map[string]string),
		wal:  w,
	}

	// Recover data from WAL if available
	if w != nil {
		entries, err := w.Recover()
		if err != nil {
			// Log error but continue - we can still operate with empty state
			log.Error("Failed to recover from WAL", "error", err)
		}

		// Apply recovered entries
		e.applyEntries(entries)
	}

	return e
}

// applyEntries applies a slice of WAL entries to the in-memory state
func (e *Engine) applyEntries(entries []*entry.Entry) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, el := range entries {
		switch el.Operation {
		case entry.OperationSet:
			e.data[el.Key] = el.Value
		case entry.OperationDelete:
			delete(e.data, el.Key)
		case entry.OperationClear:
			e.data = make(map[string]string)
		}
	}
}

// applyEntry applies a single WAL entry to the in-memory state
func (e *Engine) applyEntry(el entry.Entry) {
	e.mu.Lock()
	defer e.mu.Unlock()

	switch el.Operation {
	case entry.OperationSet:
		e.data[el.Key] = el.Value
	case entry.OperationDelete:
		delete(e.data, el.Key)
	case entry.OperationClear:
		e.data = make(map[string]string)
	}
}

// Set sets a key-value pair in the engine
func (e *Engine) Set(key, value string) error {
	// Prepare the entry
	entry := entry.Entry{
		Operation: entry.OperationSet,
		Key:       key,
		Value:     value,
	}

	// Apply the change to in-memory state
	e.applyEntry(entry)

	// Write to WAL first without holding the mutex
	if e.wal != nil {
		if err := e.wal.Write(entry); err != nil {
			return err
		}
	}

	return nil
}

// Get gets a value from the engine
func (e *Engine) Get(key string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	value, exists := e.data[key]

	return value, exists
}

// Delete deletes a key from the engine
func (e *Engine) Delete(key string) error {
	// Prepare the entry
	entry := entry.Entry{
		Operation: entry.OperationDelete,
		Key:       key,
	}

	// Write to WAL first without holding the mutex
	if e.wal != nil {
		if err := e.wal.Write(entry); err != nil {
			return err
		}
	}

	// Apply the change to in-memory state
	e.applyEntry(entry)

	return nil
}

// Clear removes all keys from the engine
func (e *Engine) Clear() error {
	// Prepare the entry
	entry := entry.Entry{
		Operation: entry.OperationClear,
	}

	// Write to WAL first without holding the mutex
	if e.wal != nil {
		if err := e.wal.Write(entry); err != nil {
			return err
		}
	}

	// Apply the change to in-memory state
	e.applyEntry(entry)

	return nil
}
