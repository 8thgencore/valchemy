//go:build !unit
// +build !unit

package replication

import (
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/8thgencore/valchemy/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSegmentPath(t *testing.T) {
	tests := []struct {
		name        string
		walDir      string
		segName     string
		expectError bool
	}{
		{
			name:        "Valid segment name",
			walDir:      "/tmp/wal",
			segName:     "wal-123.log",
			expectError: false,
		},
		{
			name:        "Invalid segment name format",
			walDir:      "/tmp/wal",
			segName:     "invalid.log",
			expectError: true,
		},
		{
			name:        "Path traversal attempt",
			walDir:      "/tmp/wal",
			segName:     "../wal-123.log",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := validateSegmentPath(tt.walDir, tt.segName)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Contains(t, path, tt.segName)
			}
		})
	}
}

func TestMasterReplication(t *testing.T) {
	walDir := t.TempDir()
	segDir := filepath.Join(walDir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	cfg := config.ReplicationConfig{
		ReplicaType:     config.Master,
		MasterHost:      "127.0.0.1",
		ReplicationPort: "13234",
	}

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	master := New(cfg, log, segDir)

	// Start master
	err := master.Start()
	require.NoError(t, err)

	// Try to connect as a client
	conn, err := net.Dial("tcp", "127.0.0.1:13234")
	require.NoError(t, err)
	defer conn.Close()

	// Verify connection
	assert.NotNil(t, conn)
}
