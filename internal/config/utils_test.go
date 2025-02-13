package config

import (
	"testing"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		input    string
		expected uint64
		wantErr  bool
	}{
		// Valid inputs
		{"10B", 10, false},
		{"2KB", 2 * 1024, false},
		{"3MB", 3 * 1024 * 1024, false},
		{"1GB", 1 * 1024 * 1024 * 1024, false},
		{" 512  GB ", 512 * 1024 * 1024 * 1024, false},
		{"1024", 1024, false},
		{"0GB", 0, false},

		// Case insensitivity
		{"10mb", 10 * 1024 * 1024, false},
		{"5gb", 5 * 1024 * 1024 * 1024, false},

		// With spaces
		{"128 MB", 128 * 1024 * 1024, false},
		{"  64  KB  ", 64 * 1024, false},

		// Edge cases
		{"18446744073709551615", 18446744073709551615, false}, // Max uint64
		{"1B", 1, false},

		// Invalid inputs
		{"10XB", 0, true},   // Invalid suffix
		{"12.3MB", 0, true}, // Float number
		{"-5KB", 0, true},   // Negative
		{"ABCD", 0, true},   // Non-numeric
		{"100PB", 0, true},  // Unsupported suffix
		{"", 0, true},       // Empty
		{"123BB", 0, true},  // Double suffix
		{"9999999999999999999999999999999GB", 0, true}, // Overflow
	}

	for _, tt := range tests {
		got, err := parseSize(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseSize(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if !tt.wantErr && got != tt.expected {
			t.Errorf("parseSize(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}
