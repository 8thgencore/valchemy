package config

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// parseSize converts a human-readable size string (e.g., "10MB", "5GB") to bytes.
// Supported suffixes: B, KB, MB, GB (case-insensitive, optional space between number and suffix).
// Returns parsed size in bytes or error if input is invalid.
func parseSize(sizeStr string) (uint64, error) {
	sizeStr = strings.ToUpper(strings.TrimSpace(sizeStr))
	sizeStr = strings.ReplaceAll(sizeStr, " ", "")

	if sizeStr == "" {
		return 0, errors.New("empty size string")
	}

	suffixes := []struct {
		suffix string
		mult   uint64
	}{
		{"GB", 1 << 30}, // 1024^3
		{"MB", 1 << 20}, // 1024^2
		{"KB", 1 << 10}, // 1024
		{"B", 1},
	}

	var numStr string
	var mult uint64 = 1
	found := false

	for _, s := range suffixes {
		if strings.HasSuffix(sizeStr, s.suffix) {
			numStr = strings.TrimSuffix(sizeStr, s.suffix)
			mult = s.mult
			found = true
			break
		}
	}

	if !found {
		if !isDigits(sizeStr) {
			return 0, fmt.Errorf("invalid format: %q", sizeStr)
		}
		numStr = sizeStr
	}

	value, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number: %q", numStr)
	}

	result := value * mult
	if value != 0 && result/value != mult { // Check for overflow
		return 0, fmt.Errorf("size overflows uint64: %q", sizeStr)
	}

	return result, nil
}

// isDigits checks if string contains only digits (0-9)
func isDigits(s string) bool {
	return regexp.MustCompile(`^\d+$`).MatchString(s)
}
