package segment

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Info represents WAL segment metadata
type Info struct {
	ID   int64
	Name string
}

// ParseSegmentName extracts segment ID from filename
func ParseSegmentName(name string) (int64, error) {
	// Extract timestamp from "wal-{timestamp}.log"
	base := strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log")
	return strconv.ParseInt(base, 10, 64)
}

// NewSegmentInfo creates a new segment info with a unique ID
func NewSegmentInfo(directory string) Info {
	id := time.Now().UnixNano()
	return Info{
		ID:   id,
		Name: fmt.Sprintf("wal-%d.log", id),
	}
}

// GetSegmentInfo creates segment info from existing segment name
func GetSegmentInfo(name string) (Info, error) {
	id, err := ParseSegmentName(name)
	if err != nil {
		return Info{}, fmt.Errorf("invalid segment name %s: %w", name, err)
	}
	return Info{
		ID:   id,
		Name: name,
	}, nil
}
