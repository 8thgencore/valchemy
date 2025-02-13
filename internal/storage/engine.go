package storage

import (
	"hash/fnv"
	"log/slog"
	"sync"

	"github.com/8thgencore/valchemy/internal/wal"
	"github.com/8thgencore/valchemy/internal/wal/entry"
)

// Engine is a struct that represents the storage engine
type Engine struct {
	partitions []*partition
	wal        wal.WAL
	numShards  int
}

type partition struct {
	data map[string]string
	mu   sync.RWMutex
}

const defaultNumShards = 16

// NewEngine creates a new Engine
func NewEngine(log *slog.Logger, w wal.WAL) *Engine {
	e := &Engine{
		partitions: make([]*partition, defaultNumShards),
		wal:        w,
		numShards:  defaultNumShards,
	}

	// Initialize partitions
	for i := 0; i < defaultNumShards; i++ {
		e.partitions[i] = &partition{
			data: make(map[string]string),
		}
	}

	// Recover data from WAL if available
	if w != nil {
		entries, err := w.Recover()
		if err != nil {
			log.Error("Failed to recover from WAL", "error", err)
		}

		// Apply recovered entries
		e.applyEntries(entries)
	}

	return e
}

// getPartition returns the partition for a given key
func (e *Engine) getPartition(key string) *partition {
	// Simple hash function to determine partition
	hash := fnv.New32a()
	hash.Write([]byte(key))

	// Ensure numShards is within the valid range for uint32
	if e.numShards < 0 || e.numShards > int(^uint32(0)) {
		return nil // or handle the error as appropriate
	}

	// Use modulo operation to get the partition index
	index := hash.Sum32() % uint32(e.numShards)

	return e.partitions[index]
}

// applyEntries applies a slice of WAL entries to the in-memory state
func (e *Engine) applyEntries(entries []*entry.Entry) {
	for _, el := range entries {
		switch el.Operation {
		case entry.OperationSet:
			p := e.getPartition(el.Key)
			p.mu.Lock()
			p.data[el.Key] = el.Value
			p.mu.Unlock()
		case entry.OperationDelete:
			p := e.getPartition(el.Key)
			p.mu.Lock()
			delete(p.data, el.Key)
			p.mu.Unlock()
		case entry.OperationClear:
			for _, p := range e.partitions {
				p.mu.Lock()
				p.data = make(map[string]string)
				p.mu.Unlock()
			}
		}
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

	// Write to WAL first
	if e.wal != nil {
		if err := e.wal.Write(entry); err != nil {
			return err
		}
	}

	// Get the appropriate partition
	p := e.getPartition(key)

	// Apply the change to in-memory state
	p.mu.Lock()
	p.data[key] = value
	p.mu.Unlock()

	return nil
}

// Get gets a value from the engine
func (e *Engine) Get(key string) (string, bool) {
	p := e.getPartition(key)
	p.mu.RLock()
	value, exists := p.data[key]
	p.mu.RUnlock()
	return value, exists
}

// Delete deletes a key from the engine
func (e *Engine) Delete(key string) error {
	// Prepare the entry
	entry := entry.Entry{
		Operation: entry.OperationDelete,
		Key:       key,
	}

	// Write to WAL first
	if e.wal != nil {
		if err := e.wal.Write(entry); err != nil {
			return err
		}
	}

	// Get the appropriate partition
	p := e.getPartition(key)

	// Apply the change to in-memory state
	p.mu.Lock()
	delete(p.data, key)
	p.mu.Unlock()

	return nil
}

// Clear removes all keys from the engine
func (e *Engine) Clear() error {
	// Prepare the entry
	entry := entry.Entry{
		Operation: entry.OperationClear,
	}

	// Write to WAL first
	if e.wal != nil {
		if err := e.wal.Write(entry); err != nil {
			return err
		}
	}

	// Clear all partitions
	for _, p := range e.partitions {
		p.mu.Lock()
		p.data = make(map[string]string)
		p.mu.Unlock()
	}

	return nil
}
