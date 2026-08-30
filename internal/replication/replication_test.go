//go:build !unit

package replication

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/8thgencore/valchemy/internal/config"
)

func TestSafeReadSegment(t *testing.T) {
	tests := []struct {
		name        string
		walDir      string
		segName     string
		content     string
		expectError bool
	}{
		{
			name:        "Valid segment name",
			walDir:      t.TempDir(),
			segName:     "wal-123.log",
			content:     "test content",
			expectError: false,
		},
		{
			name:        "Invalid segment name format",
			walDir:      t.TempDir(),
			segName:     "invalid.log",
			content:     "",
			expectError: true,
		},
		{
			name:        "Path traversal attempt",
			walDir:      t.TempDir(),
			segName:     "../wal-123.log",
			content:     "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.expectError {
				err := os.MkdirAll(tt.walDir, 0o750)
				require.NoError(t, err)

				fullPath := filepath.Join(tt.walDir, tt.segName)
				err = os.WriteFile(fullPath, []byte(tt.content), 0o600)
				require.NoError(t, err)
			}

			data, err := safeReadSegment(tt.walDir, tt.segName)
			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, data)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.content, string(data))
			}
		})
	}
}

func TestMasterReplication(t *testing.T) {
	walDir := t.TempDir()
	segDir := filepath.Join(walDir, "segments")
	require.NoError(t, os.MkdirAll(segDir, 0o750))

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
	var dialer net.Dialer

	conn, err := dialer.DialContext(context.Background(), "tcp", "127.0.0.1:13234")
	require.NoError(t, err)

	defer func() {
		if err := conn.Close(); err != nil {
			t.Logf("failed to close connection: %v", err)
		}
	}()

	// Verify connection
	assert.NotNil(t, conn)
}
