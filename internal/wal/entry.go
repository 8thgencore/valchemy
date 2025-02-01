package wal

import (
	"encoding/binary"
	"io"
)

// OperationType defines the type of operation
type OperationType byte

const (
	OperationSet    OperationType = 1
	OperationDelete OperationType = 2
)

// Entry represents a record in the WAL
type Entry struct {
	Operation OperationType
	Key       string
	Value     string
}

// WriteTo writes the entry to an io.Writer
func (e *Entry) WriteTo(w io.Writer) (int64, error) {
	var total int64

	// Write the operation type
	if n, err := w.Write([]byte{byte(e.Operation)}); err != nil {
		return total, err
	} else {
		total += int64(n)
	}

	// Write the length of the key and value
	keyLen := uint32(len(e.Key))
	if err := binary.Write(w, binary.LittleEndian, keyLen); err != nil {
		return total, err
	}
	total += 4

	// Write the key
	if n, err := w.Write([]byte(e.Key)); err != nil {
		return total, err
	} else {
		total += int64(n)
	}

	// For the SET operation, write the value
	if e.Operation == OperationSet {
		valueLen := uint32(len(e.Value))
		if err := binary.Write(w, binary.LittleEndian, valueLen); err != nil {
			return total, err
		}
		total += 4

		if n, err := w.Write([]byte(e.Value)); err != nil {
			return total, err
		} else {
			total += int64(n)
		}
	}

	return total, nil
}
