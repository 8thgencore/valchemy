package compute

import (
	"errors"

	"github.com/8thgencore/valchemy/internal/storage"
	"github.com/8thgencore/valchemy/pkg/logger"
)

type Handler struct {
	engine *storage.Engine
	log    logger.Logger
}

func NewHandler(engine *storage.Engine, log logger.Logger) *Handler {
	return &Handler{engine: engine, log: log}
}

func (h *Handler) Handle(command string) (string, error) {
	h.log.Debug("Handling command: " + command)

	// Используем ParseCommand для парсинга команды
	cmdType, args := ParseCommand(command)
	if len(args) < 1 {
		return "", errors.New("invalid command format")
	}

	switch cmdType {
	case "SET":
		if len(args) != 2 {
			return "", errors.New("invalid SET command format")
		}
		h.engine.Set(args[0], args[1])
		return "OK", nil

	case "GET":
		value, ok := h.engine.Get(args[0])
		if !ok {
			return "", errors.New("key not found")
		}
		return value, nil

	case "DEL":
		h.engine.Delete(args[0])
		return "OK", nil

	default:
		return "", errors.New("unknown command")
	}
}
