package mocks

import "github.com/8thgencore/valchemy/internal/wal/entry"

// MockSegment is a test helper that implements segment.Segment interface
type MockSegment struct {
	WriteErr         error
	SyncErr          error
	CloseErr         error
	CreateSegmentErr error
	Size_            uint64
}

func (m *MockSegment) Write(e entry.Entry) error {
	return m.WriteErr
}

func (m *MockSegment) Sync() error {
	return m.SyncErr
}

func (m *MockSegment) Close() error {
	return m.CloseErr
}

func (m *MockSegment) Size() uint64 {
	return m.Size_
}

func (m *MockSegment) CreateSegmentFile() error {
	return m.CreateSegmentErr
}
