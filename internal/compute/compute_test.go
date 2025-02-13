package compute

import (
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/storage"
	"github.com/8thgencore/valchemy/internal/wal/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTest(_ *testing.T) (*Handler, *storage.Engine, *mocks.MockWAL) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mockWAL := mocks.NewMockWAL()
	engine := storage.NewEngine(logger, mockWAL)
	handler := NewHandler(logger, engine, config.Master)

	return handler, engine, mockWAL
}

func TestReplicaHandler(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mockWAL := mocks.NewMockWAL()
	engine := storage.NewEngine(logger, mockWAL)
	handler := NewHandler(logger, engine, config.Replica)

	t.Run("Allowed commands on replica", func(t *testing.T) {
		// Test GET command
		engine.Set("key1", "value1")
		result, err := handler.Handle("GET key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", result)

		// Test HELP command
		result, err = handler.Handle("HELP")
		require.NoError(t, err)
		assert.Equal(t, HelpMessage, result)
	})

	t.Run("Forbidden commands on replica", func(t *testing.T) {
		testCases := []string{
			"SET key1 value1",
			"DEL key1",
			"CLEAR",
		}

		for _, cmd := range testCases {
			result, err := handler.Handle(cmd)
			assert.Error(t, err)
			assert.Equal(t, "replica is read-only: only GET and HELP commands are allowed", err.Error())
			assert.Empty(t, result)
		}
	})
}

func TestHandler(t *testing.T) {
	t.Run("Empty input", func(t *testing.T) {
		handler, _, _ := setupTest(t)

		result, err := handler.Handle("")
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("SET command", func(t *testing.T) {
		handler, engine, _ := setupTest(t)

		result, err := handler.Handle("SET key1 value1")
		require.NoError(t, err)
		assert.Equal(t, "OK", result)

		// Check that the value is set
		value, exists := engine.Get("key1")
		assert.True(t, exists)
		assert.Equal(t, "value1", value)
	})

	t.Run("SET command with WAL error", func(t *testing.T) {
		handler, _, mockWAL := setupTest(t)
		mockWAL.WriteError = errors.New("wal write error")

		result, err := handler.Handle("SET key1 value1")
		assert.Error(t, err)
		assert.Equal(t, "wal write error", err.Error())
		assert.Empty(t, result)
	})

	t.Run("GET command", func(t *testing.T) {
		handler, engine, _ := setupTest(t)

		// First set the value
		err := engine.Set("key1", "value1")
		require.NoError(t, err)

		result, err := handler.Handle("GET key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", result)
	})

	t.Run("DEL command", func(t *testing.T) {
		handler, engine, _ := setupTest(t)

		// First set the value
		err := engine.Set("key1", "value1")
		require.NoError(t, err)

		result, err := handler.Handle("DEL key1")
		require.NoError(t, err)
		assert.Equal(t, "OK", result)

		// Check that the value is actually deleted
		_, exists := engine.Get("key1")
		assert.False(t, exists)
	})

	t.Run("DEL command with WAL error", func(t *testing.T) {
		handler, engine, mockWAL := setupTest(t)

		// First set the value
		err := engine.Set("key1", "value1")
		require.NoError(t, err)

		// Set WAL error
		mockWAL.WriteError = errors.New("wal delete error")

		result, err := handler.Handle("DEL key1")
		assert.Error(t, err)
		assert.Equal(t, "wal delete error", err.Error())
		assert.Empty(t, result)
	})

	t.Run("GET nonexistent key", func(t *testing.T) {
		handler, _, _ := setupTest(t)

		result, err := handler.Handle("GET nonexistent")
		assert.Error(t, err)
		assert.Equal(t, "key not found", err.Error())
		assert.Empty(t, result)
	})

	t.Run("CLEAR command", func(t *testing.T) {
		handler, engine, _ := setupTest(t)

		// First add data
		err := engine.Set("key1", "value1")
		require.NoError(t, err)

		result, err := handler.Handle("CLEAR")
		require.NoError(t, err)
		assert.Equal(t, "OK", result)

		// Check that the data is cleared
		_, exists := engine.Get("key1")
		assert.False(t, exists)
	})

	t.Run("CLEAR command with WAL error", func(t *testing.T) {
		handler, engine, mockWAL := setupTest(t)

		// First add data
		err := engine.Set("key1", "value1")
		require.NoError(t, err)

		// Set WAL error
		mockWAL.WriteError = errors.New("wal clear error")

		result, err := handler.Handle("CLEAR")
		assert.Error(t, err)
		assert.Equal(t, "wal clear error", err.Error())
		assert.Empty(t, result)
	})

	t.Run("Invalid commands", func(t *testing.T) {
		handler, _, _ := setupTest(t)

		testCases := []struct {
			name     string
			command  string
			expected error
		}{
			{"Unknown command", "UNKNOWN key1", ErrUnknownCommand},
			{"SET without value", "SET key1", ErrInvalidSetFormat},
			{"GET without key", "GET", ErrInvalidFormat},
			{"DEL without key", "DEL", ErrInvalidFormat},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := handler.Handle(tc.command)
				assert.Error(t, err)
				assert.Equal(t, tc.expected, err)
				assert.Empty(t, result)
			})
		}
	})

	t.Run("HELP command", func(t *testing.T) {
		handler, _, _ := setupTest(t)

		result, err := handler.Handle("HELP")
		require.NoError(t, err)
		assert.Equal(t, HelpMessage, result)
	})

	t.Run("Unknown command", func(t *testing.T) {
		handler, _, _ := setupTest(t)

		// Test completely unknown command
		result, err := handler.Handle("UNKNOWN_COMMAND key1")
		assert.Error(t, err)
		assert.Equal(t, ErrUnknownCommand, err)
		assert.Empty(t, result)

		// Test command that doesn't match any case
		result, err = handler.Handle("NOT_A_COMMAND")
		assert.Error(t, err)
		assert.Equal(t, ErrUnknownCommand, err)
		assert.Empty(t, result)

		// Test command with extra arguments
		result, err = handler.Handle("INVALID arg1 arg2 arg3")
		assert.Error(t, err)
		assert.Equal(t, ErrUnknownCommand, err)
		assert.Empty(t, result)
	})
}
