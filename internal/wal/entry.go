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

// ReadFrom reads an entry from an io.Reader
func (e *Entry) ReadFrom(r io.Reader) (int64, error) {
	var total int64

	// Read operation type
	opByte := make([]byte, 1)
	if n, err := r.Read(opByte); err != nil {
		return total, err
	} else {
		total += int64(n)
	}
	e.Operation = OperationType(opByte[0])

	// Read key length
	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return total, err
	}
	total += 4

	// Read key
	keyBytes := make([]byte, keyLen)
	if n, err := io.ReadFull(r, keyBytes); err != nil {
		return total, err
	} else {
		total += int64(n)
	}
	e.Key = string(keyBytes)

	// For SET operations, read value
	if e.Operation == OperationSet {
		var valueLen uint32
		if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
			return total, err
		}
		total += 4

		valueBytes := make([]byte, valueLen)
		if n, err := io.ReadFull(r, valueBytes); err != nil {
			return total, err
		} else {
			total += int64(n)
		}
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
