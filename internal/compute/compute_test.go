package compute

import (
	"log/slog"
	"os"
	"testing"

	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/storage"
	"github.com/8thgencore/valchemy/pkg/logger/sl"
	"github.com/stretchr/testify/assert"
)

func TestHandler(t *testing.T) {
	cfg, err := config.NewConfig("test.yaml")
	if err != nil {
		t.Fatalf("failed to create config: %v", err)
	}
	engine, err := storage.NewEngine(cfg)
	if err != nil {
		t.Fatalf("failed to create storage engine: %v", err)
	}
	testLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	handler := NewHandler(testLogger, engine)

	t.Run("SET command", func(t *testing.T) {
		result, err := handler.Handle("SET key1 value1")
		assert.NoError(t, err)
		assert.Equal(t, "OK", result)

		// Проверяем что значение действительно установлено
		value, exists := engine.Get("key1")
		assert.True(t, exists)
		assert.Equal(t, "value1", value)
	})

	t.Run("GET command", func(t *testing.T) {
		result, err := handler.Handle("GET key1")
		assert.NoError(t, err)
		assert.Equal(t, "value1", result)
	})

	t.Run("DEL command", func(t *testing.T) {
		result, err := handler.Handle("DEL key1")
		assert.NoError(t, err)
		assert.Equal(t, "OK", result)

		// Проверяем что значение действительно удалено
		_, exists := engine.Get("key1")
		assert.False(t, exists)
	})

	t.Run("GET nonexistent key", func(t *testing.T) {
		result, err := handler.Handle("GET nonexistent")
		assert.Error(t, err)
		assert.Equal(t, "key not found", sl.Err(err))
		assert.Empty(t, result)
	})
}
