package wal

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/wal/entry"
	"github.com/8thgencore/valchemy/internal/wal/segment/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testWAL represents test helper struct
type testWAL struct {
	wal *Service
	cfg config.WALConfig

	// cleanup function
	cleanup func()
}

// setupWAL creates a new WAL instance with temporary directory for testing
func setupWAL(t *testing.T) *testWAL {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "wal_test_*")
	require.NoError(t, err)

	cfg := config.WALConfig{
		Enabled:              true,
		DataDirectory:        tempDir,
		FlushingBatchSize:    10,
		FlushingBatchTimeout: 10 * time.Millisecond,
		MaxSegmentSizeBytes:  1024,
	}

	w, err := New(cfg)
	require.NoError(t, err)

	cleanup := func() {
		if w != nil {
			w.Close()
		}
		os.RemoveAll(tempDir)
	}

	return &testWAL{
		wal:     w,
		cfg:     cfg,
		cleanup: cleanup,
	}
}

func TestNew(t *testing.T) {
	t.Run("successful creation", func(t *testing.T) {
		tw := setupWAL(t)
		defer tw.cleanup()

		assert.NotNil(t, tw.wal)
		assert.NotNil(t, tw.wal.currentSegment)
	})

	t.Run("disabled WAL", func(t *testing.T) {
		cfg := config.WALConfig{Enabled: false}
		w, err := New(cfg)

		assert.NoError(t, err)
		assert.Nil(t, w)
	})

	t.Run("invalid directory", func(t *testing.T) {
		cfg := config.WALConfig{
			Enabled:       true,
			DataDirectory: "/proc/nonexistent",
		}
		w, err := New(cfg)

		assert.Error(t, err)
		assert.Nil(t, w)
	})
}

func TestWrite(t *testing.T) {
	t.Run("immediate flush on full batch", func(t *testing.T) {
		t.Parallel()

		cfg := config.WALConfig{
			Enabled:              true,
			DataDirectory:        os.TempDir(),
			FlushingBatchSize:    2, // Small batch size for quick write
			FlushingBatchTimeout: 100 * time.Millisecond,
			MaxSegmentSizeBytes:  1024,
		}

		tempDir, err := os.MkdirTemp("", "wal_test_*")
		require.NoError(t, err)
		cfg.DataDirectory = tempDir

		w, err := New(cfg)
		require.NoError(t, err)

		cleanup := func() {
			if w != nil {
				w.Close()
			}
			os.RemoveAll(tempDir)
		}
		defer cleanup()

		// Write first entry
		err = w.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       "key1",
			Value:     "value1",
		})
		require.NoError(t, err)

		// Write second entry to trigger flush
		err = w.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       "key2",
			Value:     "value2",
		})
		require.NoError(t, err)

		// Wait for worker to process entries
		time.Sleep(50 * time.Millisecond)

		// Create new WAL instance to read entries
		w, err = New(cfg)
		require.NoError(t, err)

		// Verify entries were written correctly
		entries, err := w.Recover()
		require.NoError(t, err)
		require.Len(t, entries, 2)
		assert.Equal(t, "key1", entries[0].Key)
		assert.Equal(t, "value1", entries[0].Value)
		assert.Equal(t, "key2", entries[1].Key)
		assert.Equal(t, "value2", entries[1].Value)
	})

	t.Run("timeout based flush", func(t *testing.T) {
		t.Parallel()

		// Create configuration with short timeout and large batch size
		cfg := config.WALConfig{
			Enabled:              true,
			DataDirectory:        os.TempDir(),
			FlushingBatchSize:    100,                   // Large batch size to avoid size-based flush
			FlushingBatchTimeout: 20 * time.Millisecond, // Short timeout for quick flush
			MaxSegmentSizeBytes:  1024,
		}

		tempDir, err := os.MkdirTemp("", "wal_test_*")
		require.NoError(t, err)
		cfg.DataDirectory = tempDir

		w, err := New(cfg)
		require.NoError(t, err)

		cleanup := func() {
			if w != nil {
				w.Close()
			}
			os.RemoveAll(tempDir)
		}
		defer cleanup()

		// Write one entry
		err = w.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       "key1",
			Value:     "value1",
		})
		require.NoError(t, err)

		// Wait for timeout to occur and worker to process
		time.Sleep(50 * time.Millisecond)

		// Close WAL
		err = w.Close()
		require.NoError(t, err)

		// Create new WAL instance to read entries
		w, err = New(cfg)
		require.NoError(t, err)

		// Verify entry was written
		entries, err := w.Recover()
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "key1", entries[0].Key)
		assert.Equal(t, "value1", entries[0].Value)
	})

	t.Run("concurrent writes", func(t *testing.T) {
		t.Parallel()
		tw := setupWAL(t)
		defer tw.cleanup()

		var wg sync.WaitGroup
		numWrites := 20

		wg.Add(numWrites)
		for i := 0; i < numWrites; i++ {
			go func(i int) {
				defer wg.Done()
				err := tw.wal.Write(entry.Entry{
					Operation: entry.OperationSet,
					Key:       string(rune(i)),
					Value:     "value",
				})
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		// Wait for worker to process all entries
		time.Sleep(50 * time.Millisecond)

		// Create new WAL instance to read entries
		tw.wal.Close()
		w, err := New(tw.cfg)
		require.NoError(t, err)
		defer w.Close()

		// Verify all entries were written
		entries, err := w.Recover()
		require.NoError(t, err)
		assert.Len(t, entries, numWrites)
	})
}

func TestWrite_Errors(t *testing.T) {
	t.Run("error on segment write", func(t *testing.T) {
		tw := setupWAL(t)
		defer tw.cleanup()

		tw.wal.currentSegment = &mocks.MockSegment{
			WriteErr: errors.New("write error"),
		}

		err := tw.wal.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       "key1",
			Value:     "value1",
		})
		assert.NoError(t, err) // Write now always returns nil, as errors are handled in worker

		// Small pause to ensure worker processed the command
		time.Sleep(10 * time.Millisecond)
	})
}

func TestClose(t *testing.T) {
	t.Run("close with pending entries", func(t *testing.T) {
		t.Parallel()
		tw := setupWAL(t)

		// Write some entries
		err := tw.wal.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       "key1",
			Value:     "value1",
		})
		require.NoError(t, err)

		// Wait for worker to process entries
		time.Sleep(50 * time.Millisecond)

		// Close WAL
		err = tw.wal.Close()
		require.NoError(t, err)

		// Create new WAL instance to verify entries
		w, err := New(tw.cfg)
		require.NoError(t, err)
		defer w.Close()

		// Verify entries were flushed
		entries, err := w.Recover()
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "key1", entries[0].Key)
		assert.Equal(t, "value1", entries[0].Value)

		// Cleanup at the end
		tw.cleanup()
	})

	t.Run("multiple close calls", func(t *testing.T) {
		t.Parallel()
		tw := setupWAL(t)
		defer tw.cleanup()

		err := tw.wal.Close()
		require.NoError(t, err)

		// Second close should not panic
		err = tw.wal.Close()
		assert.Error(t, err, "second close should return error")
	})
}

func TestRecover(t *testing.T) {
	t.Run("recover with multiple segments", func(t *testing.T) {
		t.Parallel()

		// Configure WAL before creating instance
		cfg := config.WALConfig{
			Enabled:              true,
			DataDirectory:        os.TempDir(),
			FlushingBatchSize:    2,
			FlushingBatchTimeout: 100 * time.Millisecond,
			MaxSegmentSizeBytes:  50,
		}

		tempDir, err := os.MkdirTemp("", "wal_test_*")
		require.NoError(t, err)
		cfg.DataDirectory = tempDir

		// Create WAL with pre-configured configuration
		w, err := New(cfg)
		require.NoError(t, err)

		cleanup := func() {
			if w != nil {
				w.Close()
			}
			os.RemoveAll(tempDir)
		}
		defer cleanup()

		testEntries := []entry.Entry{
			{Operation: entry.OperationSet, Key: "key1", Value: "value1"},
			{Operation: entry.OperationSet, Key: "key2", Value: "value2"},
			{Operation: entry.OperationSet, Key: "key3", Value: "value3"},
			{Operation: entry.OperationSet, Key: "key4", Value: "value4"},
		}

		for _, e := range testEntries {
			err := w.Write(e)
			require.NoError(t, err)
		}

		time.Sleep(50 * time.Millisecond)

		// Close current WAL
		err = w.Close()
		require.NoError(t, err)

		// Create new WAL instance and recover
		w, err = New(cfg)
		require.NoError(t, err)

		entries, err := w.Recover()
		require.NoError(t, err)
		assert.Len(t, entries, len(testEntries))

		// Verify all entries were recovered correctly
		for i, e := range entries {
			assert.Equal(t, testEntries[i].Key, e.Key)
			assert.Equal(t, testEntries[i].Value, e.Value)
		}
	})

	t.Run("recover with empty directory", func(t *testing.T) {
		t.Parallel()
		tw := setupWAL(t)
		defer tw.cleanup()

		entries, err := tw.wal.Recover()
		require.NoError(t, err)
		assert.Empty(t, entries)
	})
}

func TestRecover_Errors(t *testing.T) {
	t.Run("error on listing segments", func(t *testing.T) {
		tw := setupWAL(t)
		defer tw.cleanup()

		// Удаляем директорию WAL
		err := os.RemoveAll(tw.cfg.DataDirectory)
		require.NoError(t, err)

		// Создаем файл вместо директории
		err = os.WriteFile(tw.cfg.DataDirectory, []byte("not a directory"), 0o644)
		require.NoError(t, err)

		entries, err := tw.wal.Recover()
		assert.Error(t, err)
		assert.Nil(t, entries)
	})

	t.Run("error on reading segment entries", func(t *testing.T) {
		tw := setupWAL(t)
		defer tw.cleanup()

		// Create a corrupted segment file
		segmentPath := filepath.Join(tw.cfg.DataDirectory, "wal-1.log")
		err := os.WriteFile(segmentPath, []byte{1, 2, 3}, 0o600)
		require.NoError(t, err)

		entries, err := tw.wal.Recover()
		assert.Error(t, err)
		assert.Nil(t, entries)
	})
}

func TestSegmentRotation(t *testing.T) {
	t.Run("automatic segment rotation", func(t *testing.T) {
		t.Parallel()

		// Configure configuration with small segment size
		cfg := config.WALConfig{
			Enabled:              true,
			DataDirectory:        os.TempDir(),
			FlushingBatchSize:    1, // Set batch size to 1 for immediate write
			FlushingBatchTimeout: 10 * time.Millisecond,
			MaxSegmentSizeBytes:  50, // Reduce segment size for guaranteed rotation
		}

		tempDir, err := os.MkdirTemp("", "wal_test_*")
		require.NoError(t, err)
		cfg.DataDirectory = tempDir

		// Create WAL with pre-configured configuration
		w, err := New(cfg)
		require.NoError(t, err)

		cleanup := func() {
			if w != nil {
				w.Close()
			}
			os.RemoveAll(tempDir)
		}
		defer cleanup()

		// Write enough data to cause rotation
		numEntries := 5
		written := make(map[string]bool)

		for i := 0; i < numEntries; i++ {
			key := string(rune(i))
			err := w.Write(entry.Entry{
				Operation: entry.OperationSet,
				Key:       key,
				Value:     "long_value_to_force_rotation",
			})
			require.NoError(t, err)
			written[key] = true

			// Check entry after each operation
			time.Sleep(20 * time.Millisecond)
			entries, err := w.Recover()
			require.NoError(t, err)

			// Check if entry was written
			found := false
			for _, e := range entries {
				if e.Key == key {
					found = true
					break
				}
			}
			require.True(t, found, "entry with key %s should be written", key)
		}

		// Close WAL to flush all buffers
		err = w.Close()
		require.NoError(t, err)

		// Create new WAL instance to read entries
		w, err = New(cfg)
		require.NoError(t, err)

		// Check if multiple segments were created
		files, err := os.ReadDir(tempDir)
		require.NoError(t, err)
		assert.Greater(t, len(files), 1, "should have created multiple segments")

		// Check if all entries can be recovered
		entries, err := w.Recover()
		require.NoError(t, err)
		assert.Len(t, entries, numEntries, "should recover all entries")

		// Check if all written keys are present
		for _, e := range entries {
			assert.True(t, written[e.Key], "entry with key %s should be in written map", e.Key)
		}
	})
}
