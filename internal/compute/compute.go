package compute

import (
	"errors"
	"log/slog"

	"github.com/8thgencore/valchemy/internal/storage"
)

// Handler is a struct that handles commands
type Handler struct {
	engine *storage.Engine
	log    *slog.Logger
}

// NewHandler creates a new Handler
func NewHandler(engine *storage.Engine, log *slog.Logger) *Handler {
	return &Handler{engine: engine, log: log}
}

// Handle handles a command
func (h *Handler) Handle(input string) (string, error) {
	h.log.Debug("Handling command: " + input)

	cmd, err := ParseCommand(input)
	if err != nil {
		return "", err
	}

	switch cmd.Type {
	case CommandSet:
		h.engine.Set(cmd.Args[0], cmd.Args[1])
		return ResponseOK, nil

	case CommandGet:
		value, ok := h.engine.Get(cmd.Args[0])
		if !ok {
			return "", errors.New("key not found")
		}
		return value, nil

	case CommandDel:
		h.engine.Delete(cmd.Args[0])
		return ResponseOK, nil
	}

	return "", ErrUnknownCommand
}
