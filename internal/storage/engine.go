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
		for _, el := range entries {
			switch el.Operation {
			case entry.OperationSet:
				e.data[el.Key] = el.Value
			case entry.OperationDelete:
				delete(e.data, el.Key)
			}
		}
	}

	return e
}

// Set sets a key-value pair in the engine
func (e *Engine) Set(key, value string) error {
	// Write to WAL
	if e.wal != nil {
		if err := e.wal.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       key,
			Value:     value,
		}); err != nil {
			return err
		}
	}

	// Update data in memory
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data[key] = value

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
	// Write to WAL
	if e.wal != nil {
		if err := e.wal.Write(entry.Entry{
			Operation: entry.OperationDelete,
			Key:       key,
		}); err != nil {
			return err
		}
	}

	// Delete from memory
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.data, key)

	return nil
}
