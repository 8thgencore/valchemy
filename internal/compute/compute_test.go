package compute

import (
	"testing"

	"github.com/8thgencore/valchemy/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Создаем мок для логгера
type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) Debug(msg string) {
	m.Called(msg)
}

func (m *MockLogger) Info(msg string) {
	m.Called(msg)
}

func (m *MockLogger) Error(msg string) {
	m.Called(msg)
}

func (m *MockLogger) Warn(msg string) {
	m.Called(msg)
}

func TestHandler(t *testing.T) {
	engine := storage.NewEngine()
	mockLogger := new(MockLogger)
	handler := NewHandler(engine, mockLogger)

	t.Run("SET command", func(t *testing.T) {
		mockLogger.On("Debug", "Handling command: SET key1 value1").Return()

		result, err := handler.Handle("SET key1 value1")
		assert.NoError(t, err)
		assert.Equal(t, "OK", result)

		// Проверяем что значение действительно установлено
		value, exists := engine.Get("key1")
		assert.True(t, exists)
		assert.Equal(t, "value1", value)

		mockLogger.AssertExpectations(t)
	})

	t.Run("GET command", func(t *testing.T) {
		mockLogger.On("Debug", "Handling command: GET key1").Return()

		result, err := handler.Handle("GET key1")
		assert.NoError(t, err)
		assert.Equal(t, "value1", result)

		mockLogger.AssertExpectations(t)
	})

	t.Run("DEL command", func(t *testing.T) {
		mockLogger.On("Debug", "Handling command: DEL key1").Return()

		result, err := handler.Handle("DEL key1")
		assert.NoError(t, err)
		assert.Equal(t, "OK", result)

		// Проверяем что значение действительно удалено
		_, exists := engine.Get("key1")
		assert.False(t, exists)

		mockLogger.AssertExpectations(t)
	})

	t.Run("GET nonexistent key", func(t *testing.T) {
		mockLogger.On("Debug", "Handling command: GET nonexistent").Return()

		result, err := handler.Handle("GET nonexistent")
		assert.Error(t, err)
		assert.Equal(t, "key not found", err.Error())
		assert.Empty(t, result)

		mockLogger.AssertExpectations(t)
	})
}
