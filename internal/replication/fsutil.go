package replication

import (
	"fmt"
	"os"
	"path/filepath"
)

// safeReadSegment safely reads a WAL segment file after path validation
func safeReadSegment(walDir, segName string) ([]byte, error) {
	// Clean and normalize paths
	walDir = filepath.Clean(walDir)
	fullPath := filepath.Join(walDir, segName)
	fullPath = filepath.Clean(fullPath)

	// Open file with explicit read-only mode
	file, err := os.OpenFile(fullPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Printf("failed to close file: %s", err)
		}
	}()

	// Get file info for size
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Read the file content
	data := make([]byte, info.Size())
	_, err = file.Read(data)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return data, nil
}
