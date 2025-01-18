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

func (h *Handler) Handle(input string) (string, error) {
	h.log.Debug("Handling command: " + input)

	cmd, err := ParseCommand(input)
	if err != nil {
		return "", err
	}

	switch cmd.Type {
	case "SET":
		h.engine.Set(cmd.Args[0], cmd.Args[1])
		return "OK", nil

	case "GET":
		value, ok := h.engine.Get(cmd.Args[0])
		if !ok {
			return "", errors.New("key not found")
		}
		return value, nil

	case "DEL":
		h.engine.Delete(cmd.Args[0])
		return "OK", nil
	}

	return "", ErrUnknownCommand
}
