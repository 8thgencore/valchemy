package segment

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/8thgencore/valchemy/internal/wal/entry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "segment_test_*")
	require.NoError(t, err)
	return dir
}

func TestNewSegment(t *testing.T) {
	t.Run("Successfully create segment", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		s, err := NewSegment(dir)
		require.NoError(t, err)
		assert.NotNil(t, s)
		assert.Contains(t, s.filename, "wal-")
		assert.Contains(t, s.filename, ".log")
	})

	t.Run("error creating directory without permissions", func(t *testing.T) {
		dir := "/proc/nonexistent" // директория, в которую точно нельзя писать
		_, err := NewSegment(dir)
		assert.Error(t, err)
	})
}

func TestSegment_CreateSegmentFile(t *testing.T) {
	t.Run("Successfully create file", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		s, err := NewSegment(dir)
		require.NoError(t, err)

		err = s.CreateSegmentFile()
		require.NoError(t, err)
		assert.NotNil(t, s.file)
		assert.NotNil(t, s.writer)

		// Check that the file exists
		_, err = os.Stat(s.filename)
		assert.NoError(t, err)
	})

	t.Run("error creating file without permissions", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "segment_test_*")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Создаем файл
		testFile := filepath.Join(tempDir, "test.log")
		err = os.WriteFile(testFile, []byte("existing file"), 0644)
		require.NoError(t, err)

		s := &segment{
			filename:  testFile,
			directory: tempDir,
		}

		// Попытка создать файл, который уже существует, должна вызвать ошибку
		err = s.CreateSegmentFile()
		assert.Error(t, err, "expected error when creating file that already exists")
	})
}

func TestSegment_Write(t *testing.T) {
	t.Run("successful write", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		s, err := NewSegment(dir)
		require.NoError(t, err)

		e := entry.Entry{
			Operation: entry.OperationSet,
			Key:       "test-key",
			Value:     "test-value",
		}

		err = s.Write(e)
		require.NoError(t, err)
		assert.Greater(t, s.Size(), uint64(0))
	})
}

func TestSegment_Sync(t *testing.T) {
	t.Run("successful synchronization", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		s, err := NewSegment(dir)
		require.NoError(t, err)

		err = s.CreateSegmentFile()
		require.NoError(t, err)

		err = s.Sync()
		assert.NoError(t, err)
	})

	t.Run("synchronization without open file", func(t *testing.T) {
		s := &segment{}
		err := s.Sync()
		assert.NoError(t, err)
	})
}

func TestListSegments(t *testing.T) {
	t.Run("successful get list of segments", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		// Create several segments
		for i := 0; i < 3; i++ {
			s, err := NewSegment(dir)
			require.NoError(t, err)
			err = s.CreateSegmentFile()
			require.NoError(t, err)
			time.Sleep(time.Millisecond) // For different timestamps
		}

		segments, err := ListSegments(dir)
		require.NoError(t, err)
		assert.Len(t, segments, 3)
	})

	t.Run("empty directory", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		segments, err := ListSegments(dir)
		require.NoError(t, err)
		assert.Empty(t, segments)
	})

	t.Run("error reading directory", func(t *testing.T) {
		_, err := ListSegments("/proc/nonexistent") // директория, которая точно не существует
		assert.Error(t, err)
	})
}

func TestReadSegmentEntries(t *testing.T) {
	t.Run("successful read entries", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		s, err := NewSegment(dir)
		require.NoError(t, err)

		// Write several entries
		testEntries := []entry.Entry{
			{Operation: entry.OperationSet, Key: "key1", Value: "value1"},
			{Operation: entry.OperationSet, Key: "key2", Value: "value2"},
		}

		for _, e := range testEntries {
			err = s.Write(e)
			require.NoError(t, err)
		}
		require.NoError(t, s.Sync())
		require.NoError(t, s.Close())

		// Read entries
		entries, err := ReadSegmentEntries(dir, filepath.Base(s.filename))
		require.NoError(t, err)
		assert.Len(t, entries, len(testEntries))
	})

	t.Run("error reading non-existent file", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		entries, err := ReadSegmentEntries(dir, "nonexistent.log")
		assert.Error(t, err)
		assert.Nil(t, entries)
	})

	t.Run("error reading corrupted file", func(t *testing.T) {
		dir := setupTestDir(t)
		defer os.RemoveAll(dir)

		// Create a corrupted file
		filename := filepath.Join(dir, "corrupted.log")
		err := os.WriteFile(filename, []byte{1, 2, 3}, 0o600)
		require.NoError(t, err)

		entries, err := ReadSegmentEntries(dir, "corrupted.log")
		assert.Error(t, err)
		assert.Nil(t, entries)
	})
}
