package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEngine(t *testing.T) {
	t.Run("NewEngine", func(t *testing.T) {
		engine := NewEngine()
		assert.NotNil(t, engine)
		assert.NotNil(t, engine.data)
	})

	t.Run("Set and Get operations", func(t *testing.T) {
		engine := NewEngine()

		// Тест установки и получения значения
		engine.Set("key1", "value1")
		value, exists := engine.Get("key1")
		assert.True(t, exists)
		assert.Equal(t, "value1", value)

		// Тест получения несуществующего ключа
		value, exists = engine.Get("nonexistent")
		assert.False(t, exists)
		assert.Empty(t, value)
	})

	t.Run("Delete operation", func(t *testing.T) {
		engine := NewEngine()

		// Установка и удаление значения
		engine.Set("key1", "value1")
		engine.Delete("key1")

		// Проверка что значение удалено
		value, exists := engine.Get("key1")
		assert.False(t, exists)
		assert.Empty(t, value)

		// Удаление несуществующего ключа не должно вызывать ошибку
		engine.Delete("nonexistent")
	})

	t.Run("Concurrent operations", func(_ *testing.T) {
		engine := NewEngine()
		done := make(chan bool)

		// Параллельные операции записи и чтения
		go func() {
			for i := 0; i < 100; i++ {
				engine.Set("key", "value")
				engine.Get("key")
			}
			done <- true
		}()

		go func() {
			for i := 0; i < 100; i++ {
				engine.Get("key")
				engine.Delete("key")
			}
			done <- true
		}()

		<-done
		<-done
	})
}
