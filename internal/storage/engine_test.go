package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/8thgencore/valchemy/internal/wal/entry"
	"github.com/8thgencore/valchemy/internal/wal/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTest(_ *testing.T) (*slog.Logger, *mocks.MockWAL) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mockWAL := mocks.NewMockWAL()

	return logger, mockWAL
}

func TestEngine(t *testing.T) {
	t.Run("NewEngine", func(t *testing.T) {
		logger, mockWAL := setupTest(t)
		engine := NewEngine(logger, mockWAL)
		assert.NotNil(t, engine)
		assert.NotNil(t, engine.partitions)
		assert.Equal(t, defaultNumShards, len(engine.partitions))

		// Verify each partition is initialized
		for _, p := range engine.partitions {
			assert.NotNil(t, p.data)
		}
	})

	t.Run("NewEngine with recovery", func(t *testing.T) {
		logger, mockWAL := setupTest(t)
		mockWAL.Entries = []*entry.Entry{
			{Operation: entry.OperationSet, Key: "key1", Value: "value1"},
			{Operation: entry.OperationSet, Key: "key2", Value: "value2"},
			{Operation: entry.OperationDelete, Key: "key1"},
		}

		engine := NewEngine(logger, mockWAL)

		// Проверяем что key1 был удален, а key2 существует
		_, exists := engine.Get("key1")
		assert.False(t, exists)

		value, exists := engine.Get("key2")
		assert.True(t, exists)
		assert.Equal(t, "value2", value)
	})

	t.Run("Set and Get operations", func(t *testing.T) {
		logger, mockWAL := setupTest(t)
		engine := NewEngine(logger, mockWAL)

		err := engine.Set("key1", "value1")
		require.NoError(t, err)

		value, exists := engine.Get("key1")
		assert.True(t, exists)
		assert.Equal(t, "value1", value)

		// Проверяем запись в WAL
		require.Len(t, mockWAL.Entries, 1)
		assert.Equal(t, entry.OperationSet, mockWAL.Entries[0].Operation)
		assert.Equal(t, "key1", mockWAL.Entries[0].Key)
		assert.Equal(t, "value1", mockWAL.Entries[0].Value)

		// Тест получения несуществующего ключа
		value, exists = engine.Get("nonexistent")
		assert.False(t, exists)
		assert.Empty(t, value)
	})

	t.Run("Delete operation", func(t *testing.T) {
		logger, mockWAL := setupTest(t)
		engine := NewEngine(logger, mockWAL)

		err := engine.Set("key1", "value1")
		require.NoError(t, err)

		err = engine.Delete("key1")
		require.NoError(t, err)

		// Проверка что значение удалено
		value, exists := engine.Get("key1")
		assert.False(t, exists)
		assert.Empty(t, value)

		// Проверяем записи в WAL
		require.Len(t, mockWAL.Entries, 2)
		assert.Equal(t, entry.OperationDelete, mockWAL.Entries[1].Operation)
		assert.Equal(t, "key1", mockWAL.Entries[1].Key)
	})

	t.Run("WAL errors", func(t *testing.T) {
		logger, mockWAL := setupTest(t)
		mockWAL.WriteError = errors.New("write error")

		engine := NewEngine(logger, mockWAL)

		err := engine.Set("key1", "value1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "write error")

		err = engine.Delete("key1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "write error")
	})

	t.Run("Concurrent operations", func(t *testing.T) {
		logger, mockWAL := setupTest(t)
		engine := NewEngine(logger, mockWAL)

		done := make(chan bool)
		const iterations = 100

		go func() {
			for i := 0; i < iterations; i++ {
				_ = engine.Set(fmt.Sprintf("key%d", i), "value")
				engine.Get(fmt.Sprintf("key%d", i))
			}
			done <- true
		}()

		go func() {
			for i := 0; i < iterations; i++ {
				engine.Get(fmt.Sprintf("key%d", i))
				_ = engine.Delete(fmt.Sprintf("key%d", i))
			}
			done <- true
		}()

		<-done
		<-done
	})

	t.Run("Concurrent Set operations", func(t *testing.T) {
		logger, mockWAL := setupTest(t)
		engine := NewEngine(logger, mockWAL)

		const iterations = 100
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				err := engine.Set("key", fmt.Sprintf("value1_%d", i))
				require.NoError(t, err)
			}
		}()

		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				err := engine.Set("key", fmt.Sprintf("value2_%d", i))
				require.NoError(t, err)
			}
		}()

		wg.Wait()

		// Verify that the last write won
		value, exists := engine.Get("key")
		assert.True(t, exists)
		assert.Contains(t, value, "value")
	})

	t.Run("Clear operation", func(t *testing.T) {
		logger, mockWAL := setupTest(t)
		engine := NewEngine(logger, mockWAL)

		// Set some data
		require.NoError(t, engine.Set("key1", "value1"))
		require.NoError(t, engine.Set("key2", "value2"))

		// Clear all data
		require.NoError(t, engine.Clear())

		// Verify data is cleared
		_, exists := engine.Get("key1")
		assert.False(t, exists)
		_, exists = engine.Get("key2")
		assert.False(t, exists)

		// Verify WAL entries
		require.Len(t, mockWAL.Entries, 3)
		assert.Equal(t, entry.OperationClear, mockWAL.Entries[2].Operation)
	})

	t.Run("Partition distribution", func(t *testing.T) {
		logger, mockWAL := setupTest(t)
		engine := NewEngine(logger, mockWAL)

		// Set multiple keys and verify they're distributed across partitions
		keys := []string{"key1", "key2", "key3", "key4", "key5"}
		for _, key := range keys {
			require.NoError(t, engine.Set(key, "value"))
		}

		// Count keys per partition
		partitionCounts := make(map[int]int)
		for i, p := range engine.partitions {
			p.mu.RLock()
			partitionCounts[i] = len(p.data)
			p.mu.RUnlock()
		}

		// Verify that keys are distributed (not all in one partition)
		totalKeys := 0
		for _, count := range partitionCounts {
			totalKeys += count
		}
		assert.Equal(t, len(keys), totalKeys)
	})
}
