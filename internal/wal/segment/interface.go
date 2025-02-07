package segment

import "github.com/8thgencore/valchemy/internal/wal/entry"

// Segment interface
type Segment interface {
	Write(entry.Entry) error
	Sync() error
	Close() error
	Size() uint64
	CreateSegmentFile() error
}
