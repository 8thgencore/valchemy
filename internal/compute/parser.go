package compute

import (
	"errors"
	"strings"
)

// Command is a struct that represents a command
type Command struct {
	Type string
	Args []string
}

// ErrInvalidFormat is an error that occurs when the command format is invalid
var ErrInvalidFormat = errors.New("invalid command format")

// ErrUnknownCommand is an error that occurs when the command is unknown
var ErrUnknownCommand = errors.New("unknown command")

// ErrInvalidSetFormat is an error that occurs when the SET command format is invalid
var ErrInvalidSetFormat = errors.New("invalid SET command format")

// ParseCommand parses a command string into a Command struct
func ParseCommand(input string) (Command, error) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return Command{}, ErrInvalidFormat
	}

	// Convert command to uppercase for case-insensitive comparison
	cmdType := strings.ToUpper(parts[0])

	// Special case for "?" as help command
	if cmdType == "?" {
		cmdType = CommandHelp
	}

	cmd := Command{
		Type: cmdType,
		Args: parts[1:],
	}

	if err := validateCommand(cmd); err != nil {
		return Command{}, err
	}

	return cmd, nil
}

// validateCommand validates a command
func validateCommand(cmd Command) error {
	switch cmd.Type {
	case CommandSet:
		if len(cmd.Args) != 2 {
			return ErrInvalidSetFormat
		}
	case CommandGet, CommandDel:
		if len(cmd.Args) != 1 {
			return ErrInvalidFormat
		}
	case CommandClear, CommandHelp, "?":
		if len(cmd.Args) != 0 {
			return ErrInvalidFormat
		}
	default:
		return ErrUnknownCommand
	}

	return nil
}
