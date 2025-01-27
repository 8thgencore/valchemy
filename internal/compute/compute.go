package compute

import (
	"errors"
	"log/slog"

	"github.com/8thgencore/valchemy/internal/storage"
)

// Handler is a struct that handles commands
type Handler struct {
	log    *slog.Logger
	engine *storage.Engine
}

// NewHandler creates a new Handler
func NewHandler(log *slog.Logger, engine *storage.Engine) *Handler {
	return &Handler{log: log, engine: engine}
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
