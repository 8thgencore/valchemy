package storage

import "sync"

type Engine struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewEngine() *Engine {
	return &Engine{
		data: make(map[string]string),
	}
}

func (e *Engine) Set(key, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data[key] = value
}

func (e *Engine) Get(key string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	value, exists := e.data[key]

	return value, exists
}

func (e *Engine) Delete(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.data, key)
}
