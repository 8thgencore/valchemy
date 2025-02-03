package storage

import (
	"errors"
	"log/slog"
	"os"
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
		assert.NotNil(t, engine.data)
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
				_ = engine.Set("key", "value")
				engine.Get("key")
			}
			done <- true
		}()

		go func() {
			for i := 0; i < iterations; i++ {
				engine.Get("key")
				_ = engine.Delete("key")
			}
			done <- true
		}()

		<-done
		<-done
	})
}
