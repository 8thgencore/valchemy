package storage

import "sync"

// Engine is a struct that represents the storage engine
type Engine struct {
	data map[string]string
	mu   sync.RWMutex
}

// NewEngine creates a new Engine
func NewEngine() *Engine {
	return &Engine{
		data: make(map[string]string),
	}
}

// Set sets a key-value pair in the engine
func (e *Engine) Set(key, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data[key] = value
}

// Get gets a value from the engine
func (e *Engine) Get(key string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	value, exists := e.data[key]

	return value, exists
}

// Delete deletes a key from the engine
func (e *Engine) Delete(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.data, key)
}
