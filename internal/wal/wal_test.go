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
	wal     *Service
	cfg     config.WALConfig
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
		// First remove the directory, then close WAL
		os.RemoveAll(tempDir)
		if w != nil {
			w.Close()
		}
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
			DataDirectory: "/nonexistent/directory",
		}
		w, err := New(cfg)

		assert.Error(t, err)
		assert.Nil(t, w)
	})
}

func TestWrite(t *testing.T) {
	t.Run("immediate flush on full batch", func(t *testing.T) {
		t.Parallel()
		tw := setupWAL(t)
		defer tw.cleanup()

		tw.wal.config.FlushingBatchSize = 2
		tw.wal.config.FlushingBatchTimeout = 100 * time.Millisecond

		// Write first entry
		err := tw.wal.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       "key1",
			Value:     "value1",
		})
		require.NoError(t, err)

		// Verify first entry was written correctly
		entries, err := tw.wal.Recover()
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "key1", entries[0].Key)
		assert.Equal(t, "value1", entries[0].Value)

		// Write second entry to trigger flush
		err = tw.wal.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       "key2",
			Value:     "value2",
		})
		require.NoError(t, err)

		// Verify both entries were written correctly
		entries, err = tw.wal.Recover()
		require.NoError(t, err)
		require.Len(t, entries, 2)
		assert.Equal(t, "key1", entries[0].Key)
		assert.Equal(t, "value1", entries[0].Value)
		assert.Equal(t, "key2", entries[1].Key)
		assert.Equal(t, "value2", entries[1].Value)
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

		// Wait for any pending flushes
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

	t.Run("timeout based flush", func(t *testing.T) {
		t.Parallel()
		tw := setupWAL(t)
		defer tw.cleanup()

		tw.wal.config.FlushingBatchTimeout = 20 * time.Millisecond
		tw.wal.config.FlushingBatchSize = 100 // Large enough to not trigger size-based flush

		err := tw.wal.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       "key1",
			Value:     "value1",
		})
		require.NoError(t, err)

		// Wait for timeout to occur
		time.Sleep(30 * time.Millisecond)

		// Verify entry was written
		entries, err := tw.wal.Recover()
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, "key1", entries[0].Key)
		assert.Equal(t, "value1", entries[0].Value)
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
		assert.Error(t, err)
		assert.ErrorIs(t, errors.Unwrap(err), ErrFlushWAL)
	})

	t.Run("error on segment sync", func(t *testing.T) {
		tw := setupWAL(t)
		defer tw.cleanup()

		tw.wal.currentSegment = &mocks.MockSegment{
			SyncErr: errors.New("sync error"),
		}
		tw.wal.config.FlushingBatchSize = 1

		err := tw.wal.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       "key1",
			Value:     "value1",
		})
		assert.Error(t, err)
		assert.ErrorIs(t, errors.Unwrap(err), ErrSyncWAL)
	})

	t.Run("error on segment rotation", func(t *testing.T) {
		tw := setupWAL(t)
		defer tw.cleanup()

		tw.wal.config.MaxSegmentSizeBytes = 1
		tw.wal.currentSegment = &mocks.MockSegment{
			Size_:    2,
			CloseErr: errors.New("close error"),
		}

		err := tw.wal.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       "key1",
			Value:     "value1",
		})
		assert.Error(t, err)
		assert.ErrorIs(t, errors.Unwrap(err), ErrFlushWAL)
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
		assert.Len(t, entries, 1)
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

func TestClose_Errors(t *testing.T) {
	t.Run("error on final batch flush", func(t *testing.T) {
		tw := setupWAL(t)
		defer tw.cleanup()

		tw.wal.currentSegment = &mocks.MockSegment{
			SyncErr: errors.New("sync error"),
		}

		// Write an entry to create a batch
		_ = tw.wal.Write(entry.Entry{
			Operation: entry.OperationSet,
			Key:       "key1",
			Value:     "value1",
		})

		err := tw.wal.Close()
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrFlushFinalBatch)
	})

	t.Run("error on segment close", func(t *testing.T) {
		tw := setupWAL(t)
		defer tw.cleanup()

		tw.wal.currentSegment = &mocks.MockSegment{
			CloseErr: ErrCloseSegment,
		}

		err := tw.wal.Close()
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrCloseSegment)
	})
}

func TestRecover(t *testing.T) {
	t.Run("recover with multiple segments", func(t *testing.T) {
		t.Parallel()
		tw := setupWAL(t)
		defer tw.cleanup()

		tw.wal.config.MaxSegmentSizeBytes = 50
		tw.wal.config.FlushingBatchSize = 2
		tw.wal.config.FlushingBatchTimeout = 100 * time.Millisecond

		testEntries := []entry.Entry{
			{Operation: entry.OperationSet, Key: "key1", Value: "value1"},
			{Operation: entry.OperationSet, Key: "key2", Value: "value2"},
			{Operation: entry.OperationSet, Key: "key3", Value: "value3"},
			{Operation: entry.OperationSet, Key: "key4", Value: "value4"},
		}

		for _, e := range testEntries {
			err := tw.wal.Write(e)
			require.NoError(t, err)
		}

		time.Sleep(50 * time.Millisecond)

		// Close current WAL
		err := tw.wal.Close()
		require.NoError(t, err)

		// Create new WAL instance and recover
		w, err := New(tw.cfg)
		require.NoError(t, err)
		defer w.Close()

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

		// Remove directory permissions
		err := os.Chmod(tw.cfg.DataDirectory, 0o000)
		require.NoError(t, err)
		defer os.Chmod(tw.cfg.DataDirectory, 0o750)

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
		tw := setupWAL(t)
		defer tw.cleanup()

		tw.wal.config.MaxSegmentSizeBytes = 50
		numEntries := 5

		// Write entries until rotation occurs
		for i := 0; i < numEntries; i++ {
			err := tw.wal.Write(entry.Entry{
				Operation: entry.OperationSet,
				Key:       string(rune(i)),
				Value:     "long_value_to_force_rotation",
			})
			require.NoError(t, err)
		}

		// Wait for any pending flushes
		time.Sleep(50 * time.Millisecond)

		// Verify multiple segments were created
		files, err := os.ReadDir(tw.cfg.DataDirectory)
		require.NoError(t, err)
		assert.Greater(t, len(files), 1)

		// Verify all entries can be recovered
		entries, err := tw.wal.Recover()
		require.NoError(t, err)
		assert.Len(t, entries, numEntries)
	})
}
