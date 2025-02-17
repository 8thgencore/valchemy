package mocks

import (
	"sync"

	"github.com/8thgencore/valchemy/internal/wal"
	"github.com/8thgencore/valchemy/internal/wal/entry"
)

type MockWAL struct {
	*wal.Service
	Entries    []*entry.Entry
	WriteError error
	CloseError error
	RecoverErr error
	mu         sync.Mutex
}

func NewMockWAL() *MockWAL {
	return &MockWAL{
		Entries: make([]*entry.Entry, 0),
	}
}

func (m *MockWAL) Write(e entry.Entry) error {
	if m.WriteError != nil {
		return m.WriteError
	}
	m.mu.Lock()
	m.Entries = append(m.Entries, &e)
	m.mu.Unlock()
	return nil
}

func (m *MockWAL) Close() error {
	return m.CloseError
}

func (m *MockWAL) Recover() ([]*entry.Entry, error) {
	if m.RecoverErr != nil {
		return nil, m.RecoverErr
	}
	m.mu.Lock()
	entries := m.Entries
	m.mu.Unlock()
	return entries, nil
}
