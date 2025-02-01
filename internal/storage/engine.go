package storage

import (
	"sync"

	"github.com/8thgencore/valchemy/internal/wal"
)

// Engine is a struct that represents the storage engine
type Engine struct {
	data map[string]string
	mu   sync.RWMutex
	wal  *wal.WAL
}

// NewEngine creates a new Engine
func NewEngine(wal *wal.WAL) *Engine {
	return &Engine{
		data: make(map[string]string),
		wal:  wal,
	}
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
