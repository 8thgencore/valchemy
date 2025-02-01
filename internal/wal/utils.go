package wal

import (
	"strconv"
	"strings"
)

// parseSize converts a human-readable size string (e.g., "10MB") to bytes
func parseSize(size string) int64 {
	var multiplier int64 = 1
	size = strings.TrimSpace(size)

	switch {
	case strings.HasSuffix(size, "KB"):
		multiplier = 1024
		size = strings.TrimSuffix(size, "KB")
	case strings.HasSuffix(size, "MB"):
		multiplier = 1024 * 1024
		size = strings.TrimSuffix(size, "MB")
	case strings.HasSuffix(size, "GB"):
		multiplier = 1024 * 1024 * 1024
		size = strings.TrimSuffix(size, "GB")
	}

	value, _ := strconv.ParseInt(size, 10, 64)
	return value * multiplier
}
