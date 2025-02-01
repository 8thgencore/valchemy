package wal

import "github.com/8thgencore/valchemy/internal/wal/entry"

// WAL represents the interface for Write-Ahead Log operations
type WAL interface {
	Write(entry entry.Entry) error
	Close() error
	Recover() ([]*entry.Entry, error)
}
