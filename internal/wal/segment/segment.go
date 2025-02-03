package segment

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/8thgencore/valchemy/internal/wal/entry"
)

// Segment represents a WAL Segment file
type Segment struct {
	file      *os.File
	writer    *bufio.Writer
	size      uint64
	filename  string
	directory string
}

// NewSegment creates a new WAL segment
func NewSegment(directory string) (*Segment, error) {
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	filename := filepath.Join(
		directory,
		fmt.Sprintf("wal-%d.log", time.Now().UnixNano()),
	)

	return &Segment{
		filename:  filename,
		directory: directory,
	}, nil
}

// CreateSegmentFile creates a new segment file if it doesn't exist
func (s *Segment) CreateSegmentFile() error {
	if s.file == nil {
		file, err := os.OpenFile(s.filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return fmt.Errorf("failed to create segment file: %w", err)
		}
		s.file = file
		s.writer = bufio.NewWriter(file)
	}

	return nil
}

// Write writes data to the segment and updates its size
func (s *Segment) Write(entry entry.Entry) error {
	if err := s.CreateSegmentFile(); err != nil {
		return fmt.Errorf("failed to create segment file: %w", err)
	}

	n, err := entry.WriteTo(s.writer)
	if err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	if n < 0 {
		return fmt.Errorf("negative write size: %d", n)
	}
	newSize := s.size + uint64(n)
	if newSize < s.size {
		return errors.New("size overflow detected")
	}
	s.size = newSize

	return nil
}

// Sync ensures all data is written to disk
func (s *Segment) Sync() error {
	if s.file == nil {
		return nil
	}

	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	return s.file.Sync()
}

// Close closes the segment file
func (s *Segment) Close() error {
	if s.file == nil {
		return nil
	}

	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer on close: %w", err)
	}

	return s.file.Close()
}

// Size returns the size of the segment
func (s *Segment) Size() uint64 {
	return s.size
}

// ListSegments returns a sorted list of WAL segment files
func ListSegments(directory string) ([]string, error) {
	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var segments []string
	for _, f := range files {
		name := f.Name()
		if !strings.Contains(name, "..") &&
			strings.HasPrefix(name, "wal-") &&
			strings.HasSuffix(name, ".log") {
			segments = append(segments, name)
		}
	}
	sort.Strings(segments)

	return segments, nil
}

// ReadSegmentEntries reads all entries from the given segment file
func ReadSegmentEntries(directory, segmentName string) ([]*entry.Entry, error) {
	// Validate and sanitize the input paths
	segmentPath := filepath.Join(directory, segmentName)
	segmentPath = filepath.Clean(segmentPath)

	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment %s: %w", segmentName, err)
	}

	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			err = fmt.Errorf("failed to close segment file: %w", closeErr)
		}
	}()

	var entries []*entry.Entry
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
