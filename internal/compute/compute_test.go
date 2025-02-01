package compute

import (
	"log/slog"
	"os"
	"testing"

	"github.com/8thgencore/valchemy/internal/storage"
	"github.com/8thgencore/valchemy/internal/wal/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTest(_ *testing.T) (*Handler, *storage.Engine) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mockWAL := mocks.NewMockWAL()
	engine := storage.NewEngine(logger, mockWAL)
	handler := NewHandler(logger, engine)

	return handler, engine
}

func TestHandler(t *testing.T) {
	t.Run("SET command", func(t *testing.T) {
		handler, engine := setupTest(t)

		result, err := handler.Handle("SET key1 value1")
		require.NoError(t, err)
		assert.Equal(t, "OK", result)

		// Проверяем что значение действительно установлено
		value, exists := engine.Get("key1")
		assert.True(t, exists)
		assert.Equal(t, "value1", value)
	})

	t.Run("GET command", func(t *testing.T) {
		handler, engine := setupTest(t)

		// Сначала устанавливаем значение
		err := engine.Set("key1", "value1")
		require.NoError(t, err)

		result, err := handler.Handle("GET key1")
		require.NoError(t, err)
		assert.Equal(t, "value1", result)
	})

	t.Run("DEL command", func(t *testing.T) {
		handler, engine := setupTest(t)

		// Сначала устанавливаем значение
		err := engine.Set("key1", "value1")
		require.NoError(t, err)

		result, err := handler.Handle("DEL key1")
		require.NoError(t, err)
		assert.Equal(t, "OK", result)

		// Проверяем что значение действительно удалено
		_, exists := engine.Get("key1")
		assert.False(t, exists)
	})

	t.Run("GET nonexistent key", func(t *testing.T) {
		handler, _ := setupTest(t)

		result, err := handler.Handle("GET nonexistent")
		assert.Error(t, err)
		assert.Equal(t, "key not found", err.Error())
		assert.Empty(t, result)
	})

	t.Run("Invalid commands", func(t *testing.T) {
		handler, _ := setupTest(t)

		testCases := []struct {
			name    string
			command string
		}{
			{"Empty command", ""},
			{"Unknown command", "UNKNOWN key1"},
			{"SET without value", "SET key1"},
			{"GET without key", "GET"},
			{"DEL without key", "DEL"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := handler.Handle(tc.command)
				assert.Error(t, err)
				assert.Empty(t, result)
			})
		}
	})

	t.Run("HELP command", func(t *testing.T) {
		handler, _ := setupTest(t)

		result, err := handler.Handle("HELP")
		require.NoError(t, err)
		assert.Equal(t, HelpMessage, result)
	})
}
