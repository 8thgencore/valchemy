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
	n, err := w.Write([]byte{byte(e.Operation)})
	if err != nil {
		return total, err
	}
	total += int64(n)

	// Write the length of the key using a preallocated buffer
	keyLen := uint32(len(e.Key))
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, keyLen)
	n, err = w.Write(buf)
	if err != nil {
		return total, err
	}
	total += int64(n)

	// Write the key
	n, err = w.Write([]byte(e.Key))
	if err != nil {
		return total, err
	}
	total += int64(n)

	// For the SET operation, write the value
	if e.Operation == OperationSet {
		valueLen := uint32(len(e.Value))
		binary.LittleEndian.PutUint32(buf, valueLen)
		n, err = w.Write(buf)
		if err != nil {
			return total, err
		}
		total += int64(n)

		n, err = w.Write([]byte(e.Value))
		if err != nil {
			return total, err
		}
		total += int64(n)
	}

	return total, nil
}

// ReadFrom reads an entry from an io.Reader
func (e *Entry) ReadFrom(r io.Reader) (int64, error) {
	var total int64

	// Read operation type
	opByte := make([]byte, 1)
	n, err := io.ReadFull(r, opByte)
	if err != nil {
		return total, err
	}
	total += int64(n)
	e.Operation = OperationType(opByte[0])

	// Read key length using a preallocated buffer
	buf := make([]byte, 4)
	n, err = io.ReadFull(r, buf)
	if err != nil {
		return total, err
	}
	total += int64(n)
	keyLen := binary.LittleEndian.Uint32(buf)

	// Read key
	keyBytes := make([]byte, keyLen)
	n, err = io.ReadFull(r, keyBytes)
	if err != nil {
		return total, err
	}
	total += int64(n)
	e.Key = string(keyBytes)

	// For SET operations, read value
	if e.Operation == OperationSet {
		n, err = io.ReadFull(r, buf)
		if err != nil {
			return total, err
		}
		total += int64(n)
		valueLen := binary.LittleEndian.Uint32(buf)

		valueBytes := make([]byte, valueLen)
		n, err = io.ReadFull(r, valueBytes)
		if err != nil {
			return total, err
		}
		total += int64(n)
		e.Value = string(valueBytes)
	}

	return total, nil
}

// readEntry reads a single entry from the WAL file
func readEntry(r io.Reader) (*Entry, error) {
	entry := &Entry{}
	if _, err := entry.ReadFrom(r); err != nil {
		return nil, err
	}

	return entry, nil
}
