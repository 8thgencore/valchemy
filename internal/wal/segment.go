package wal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/8thgencore/valchemy/internal/wal/entry"
)

// segment represents a WAL segment file
type segment struct {
	file     *os.File
	writer   *bufio.Writer
	size     uint64
	filename string
}

// newSegment creates a new WAL segment
func newSegment(directory string) (*segment, error) {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	filename := filepath.Join(
		directory,
		fmt.Sprintf("wal-%d.log", time.Now().UnixNano()),
	)

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment: %w", err)
	}

	writer := bufio.NewWriter(file)

	return &segment{
		file:     file,
		writer:   writer,
		filename: filename,
	}, nil
}

// write writes data to the segment and updates its size
func (s *segment) write(entry entry.Entry) error {
	n, err := entry.WriteTo(s.writer)
	if err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}
	s.size += uint64(n)

	return nil
}

// sync ensures all data is written to disk
func (s *segment) sync() error {
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	return s.file.Sync()
}

// close closes the segment file
func (s *segment) close() error {
	_ = s.writer.Flush()
	return s.file.Close()
}

// listSegments returns a sorted list of WAL segment files
func listSegments(directory string) ([]string, error) {
	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var segments []string
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "wal-") && strings.HasSuffix(f.Name(), ".log") {
			segments = append(segments, f.Name())
		}
	}
	sort.Strings(segments)

	return segments, nil
}

// readSegmentEntries reads all entries from the given segment file
func readSegmentEntries(directory, segmentName string) ([]*entry.Entry, error) {
	var entries []*entry.Entry

	file, err := os.Open(filepath.Join(directory, segmentName))
	if err != nil {
		return nil, fmt.Errorf("failed to open segment %s: %w", segmentName, err)
	}
	defer file.Close()

	for {
		entry, err := entry.ReadEntry(file)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read entry from segment %s: %w", segmentName, err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}
