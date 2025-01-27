package compute

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantCmd Command
		wantErr error
	}{
		{
			name:    "Empty command",
			input:   "",
			wantErr: ErrInvalidFormat,
		},
		{
			name:    "Command without arguments",
			input:   "GET",
			wantErr: ErrInvalidFormat,
		},
		{
			name:  "Valid GET command",
			input: "GET key1",
			wantCmd: Command{
				Type: "GET",
				Args: []string{"key1"},
			},
		},
		{
			name:  "Valid SET command",
			input: "SET key1 value1",
			wantCmd: Command{
				Type: "SET",
				Args: []string{"key1", "value1"},
			},
		},
		{
			name:    "Invalid SET command",
			input:   "SET key1",
			wantErr: ErrInvalidSetFormat,
		},
		{
			name:  "Valid DEL command",
			input: "DEL key1",
			wantCmd: Command{
				Type: "DEL",
				Args: []string{"key1"},
			},
		},
		{
			name:    "Unknown command",
			input:   "UNKNOWN key1",
			wantErr: ErrUnknownCommand,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := ParseCommand(tt.input)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.wantCmd, cmd)
		})
	}
}
