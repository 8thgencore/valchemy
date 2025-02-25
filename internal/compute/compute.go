package compute

import (
	"log/slog"

	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/storage"
)

// Handler is a struct that handles commands
type Handler struct {
	log         *slog.Logger
	engine      *storage.Engine
	replicaType config.ReplicationType
}

// NewHandler creates a new Handler
func NewHandler(log *slog.Logger, engine *storage.Engine, replicaType config.ReplicationType) *Handler {
	return &Handler{
		log:         log,
		engine:      engine,
		replicaType: replicaType,
	}
}

// Handle handles a command string
func (h *Handler) Handle(input string) (string, error) {
	// If input is empty, do nothing
	if input == "" {
		return "", nil
	}

	h.log.Debug("Handling command: " + input)

	cmd, err := ParseCommand(input)
	if err != nil {
		return "", err
	}

	return h.handleCommand(cmd)
}

// handleCommand handles a parsed command (exported for testing)
func (h *Handler) handleCommand(cmd Command) (string, error) {
	// Check if we're on replica and command is not allowed
	if h.replicaType == config.Replica {
		switch cmd.Type {
		case CommandHelp, CommandGet:
			// These commands are allowed
		default:
			return "", ErrReadOnlyReplica
		}
	}

	switch cmd.Type {
	case CommandHelp:
		return HelpMessage, nil

	case CommandSet:
		if err := h.engine.Set(cmd.Args[0], cmd.Args[1]); err != nil {
			return "", err
		}
		return ResponseOK, nil

	case CommandGet:
		value, ok := h.engine.Get(cmd.Args[0])
		if !ok {
			return "", ErrKeyNotFound
		}
		return value, nil

	case CommandDel:
		if err := h.engine.Delete(cmd.Args[0]); err != nil {
			return "", err
		}
		return ResponseOK, nil

	case CommandClear:
		if err := h.engine.Clear(); err != nil {
			return "", err
		}
		return ResponseOK, nil
	}

	return "", ErrUnknownCommand
}
