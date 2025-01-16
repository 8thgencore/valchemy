package compute

import (
	"errors"
	"strings"

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

	parts := strings.Fields(command)
	if len(parts) < 2 {
		return "", errors.New("invalid command format")
	}

	switch parts[0] {
	case "SET":
		if len(parts) != 3 {
			return "", errors.New("invalid SET command format")
		}
		h.engine.Set(parts[1], parts[2])
		return "OK", nil

	case "GET":
		value, ok := h.engine.Get(parts[1])
		if !ok {
			return "", errors.New("key not found")
		}
		return value, nil

	case "DEL":
		h.engine.Delete(parts[1])
		return "OK", nil

	default:
		return "", errors.New("unknown command")
	}
}
