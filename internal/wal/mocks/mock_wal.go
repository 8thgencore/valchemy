package mocks

import (
	"github.com/8thgencore/valchemy/internal/wal"
	"github.com/8thgencore/valchemy/internal/wal/entry"
)

type MockWAL struct {
	*wal.Service
	Entries    []*entry.Entry
	WriteError error
	CloseError error
	RecoverErr error
}

func NewMockWAL() *MockWAL {
	return &MockWAL{
		Entries: make([]*entry.Entry, 0),
	}
}

func (m *MockWAL) Write(entry entry.Entry) error {
	if m.WriteError != nil {
		return m.WriteError
	}
	m.Entries = append(m.Entries, &entry)
	return nil
}

func (m *MockWAL) Close() error {
	return m.CloseError
}

func (m *MockWAL) Recover() ([]*entry.Entry, error) {
	if m.RecoverErr != nil {
		return nil, m.RecoverErr
	}
	return m.Entries, nil
}
