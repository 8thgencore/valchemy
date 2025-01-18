package compute

import (
	"errors"
	"strings"
)

type Command struct {
	Type string
	Args []string
}

var (
	ErrInvalidFormat    = errors.New("invalid command format")
	ErrUnknownCommand   = errors.New("unknown command")
	ErrInvalidSetFormat = errors.New("invalid SET command format")
)

// ParseCommand парсит строку команды в структуру
func ParseCommand(input string) (Command, error) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return Command{}, ErrInvalidFormat
	}

	cmd := Command{
		Type: parts[0],
		Args: parts[1:],
	}

	if err := validateCommand(cmd); err != nil {
		return Command{}, err
	}

	return cmd, nil
}

func validateCommand(cmd Command) error {
	if len(cmd.Args) < 1 {
		return ErrInvalidFormat
	}

	switch cmd.Type {
	case "SET":
		if len(cmd.Args) != 2 {
			return ErrInvalidSetFormat
		}
	case "GET", "DEL":
		// Эти команды требуют только один аргумент
	default:
		return ErrUnknownCommand
	}

	return nil
}
