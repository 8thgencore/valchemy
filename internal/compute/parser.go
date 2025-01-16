package compute

import (
	"strings"
)

// Функция для парсинга команды
func ParseCommand(command string) (string, []string) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return "", nil
	}
	return parts[0], parts[1:]
}
