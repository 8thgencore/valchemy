package wal

import (
	"os"
	"testing"
	"time"

	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/wal/entry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupWALTest(t *testing.T) (config.WALConfig, func()) {
	tempDir, err := os.MkdirTemp("", "wal_test_*")
	require.NoError(t, err)

	cfg := config.WALConfig{
		Enabled:              true,
		DataDirectory:        tempDir,
		FlushingBatchSize:    10,
		FlushingBatchTimeout: time.Second,
		MaxSegmentSizeBytes:  1024,
	}

	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return cfg, cleanup
}

func TestWAL(t *testing.T) {
	t.Run("New WAL", func(t *testing.T) {
		cfg, cleanup := setupWALTest(t)
		defer cleanup()

		w, err := New(cfg)
		require.NoError(t, err)
		assert.NotNil(t, w)
		assert.NotNil(t, w.currentSegment)

		err = w.Close()
		require.NoError(t, err)
	})

	t.Run("Write and Recover", func(t *testing.T) {
		cfg, cleanup := setupWALTest(t)
		defer cleanup()

		w, err := New(cfg)
		require.NoError(t, err)

		// Записываем несколько операций
		testEntries := []entry.Entry{
			{Operation: entry.OperationSet, Key: "key1", Value: "value1"},
			{Operation: entry.OperationSet, Key: "key2", Value: "value2"},
			{Operation: entry.OperationDelete, Key: "key1"},
		}

		for _, entry := range testEntries {
			err = w.Write(entry)
			require.NoError(t, err)
		}

		// Закрываем WAL
		err = w.Close()
		require.NoError(t, err)

		// Создаем новый WAL и восстанавливаем данные
		w, err = New(cfg)
		require.NoError(t, err)

		entries, err := w.Recover()
		require.NoError(t, err)
		require.Len(t, entries, len(testEntries))

		// Проверяем восстановленные записи
		for i, entry := range entries {
			assert.Equal(t, testEntries[i].Operation, entry.Operation)
			assert.Equal(t, testEntries[i].Key, entry.Key)
			assert.Equal(t, testEntries[i].Value, entry.Value)
		}

		err = w.Close()
		require.NoError(t, err)
	})

	t.Run("Segment Rotation", func(t *testing.T) {
		cfg, cleanup := setupWALTest(t)
		cfg.MaxSegmentSizeBytes = 100 // Маленький размер для быстрой ротации
		defer cleanup()

		w, err := New(cfg)
		require.NoError(t, err)

		// Записываем много данных для вызова ротации
		for i := 0; i < 50; i++ {
			err = w.Write(entry.Entry{
				Operation: entry.OperationSet,
				Key:       "key",
				Value:     "long_value_to_force_rotation",
			})
			require.NoError(t, err)
		}

		// Проверяем что создано несколько сегментов
		files, err := os.ReadDir(cfg.DataDirectory)
		require.NoError(t, err)
		assert.Greater(t, len(files), 1)

		err = w.Close()
		require.NoError(t, err)
	})

	t.Run("Batch Flushing", func(t *testing.T) {
		cfg, cleanup := setupWALTest(t)
		cfg.FlushingBatchSize = 3
		defer cleanup()

		w, err := New(cfg)
		require.NoError(t, err)

		// Записываем данные меньше размера батча
		err = w.Write(entry.Entry{Operation: entry.OperationSet, Key: "key1", Value: "value1"})
		require.NoError(t, err)

		// Проверяем что данные в батче
		w.batchMu.Lock()
		assert.Len(t, w.batch, 1)
		w.batchMu.Unlock()

		// Записываем достаточно данных для автоматического сброса
		err = w.Write(entry.Entry{Operation: entry.OperationSet, Key: "key2", Value: "value2"})
		require.NoError(t, err)
		err = w.Write(entry.Entry{Operation: entry.OperationSet, Key: "key3", Value: "value3"})
		require.NoError(t, err)

		// Проверяем что батч пуст после автоматического сброса
		w.batchMu.Lock()
		assert.Len(t, w.batch, 0)
		w.batchMu.Unlock()

		err = w.Close()
		require.NoError(t, err)
	})

	t.Run("Timeout Flushing", func(t *testing.T) {
		cfg, cleanup := setupWALTest(t)
		cfg.FlushingBatchTimeout = 100 * time.Millisecond
		cfg.FlushingBatchSize = 1000 // Большой размер, чтобы сработал таймаут
		defer cleanup()

		w, err := New(cfg)
		require.NoError(t, err)

		err = w.Write(entry.Entry{Operation: entry.OperationSet, Key: "key1", Value: "value1"})
		require.NoError(t, err)

		// Ждем срабатывания таймаута
		time.Sleep(200 * time.Millisecond)

		// Проверяем что батч пуст после сброса по таймауту
		w.batchMu.Lock()
		assert.Len(t, w.batch, 0)
		w.batchMu.Unlock()

		err = w.Close()
		require.NoError(t, err)
	})

	t.Run("WAL Disabled", func(t *testing.T) {
		cfg, cleanup := setupWALTest(t)
		cfg.Enabled = false
		defer cleanup()

		w, err := New(cfg)
		assert.NoError(t, err)
		assert.Nil(t, w)
	})
}
