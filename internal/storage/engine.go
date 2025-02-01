package storage

import (
	"log/slog"
	"sync"

	"github.com/8thgencore/valchemy/internal/wal"
)

// Engine is a struct that represents the storage engine
type Engine struct {
	log  *slog.Logger
	data map[string]string
	mu   sync.RWMutex
	wal  *wal.WAL
}

// NewEngine creates a new Engine
func NewEngine(log *slog.Logger, w *wal.WAL) *Engine {
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
		for _, entry := range entries {
			switch entry.Operation {
			case wal.OperationSet:
				e.data[entry.Key] = entry.Value
			case wal.OperationDelete:
				delete(e.data, entry.Key)
			}
		}
	}

	return e
}

// Set sets a key-value pair in the engine
func (e *Engine) Set(key, value string) error {
	// Сначала записываем в WAL
	if e.wal != nil {
		if err := e.wal.Write(wal.Entry{
			Operation: wal.OperationSet,
			Key:       key,
			Value:     value,
		}); err != nil {
			return err
		}
	}

	// Затем обновляем данные в памяти
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
	// Сначала записываем в WAL
	if e.wal != nil {
		if err := e.wal.Write(wal.Entry{
			Operation: wal.OperationDelete,
			Key:       key,
		}); err != nil {
			return err
		}
	}

	// Затем удаляем из памяти
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.data, key)
	return nil
}
