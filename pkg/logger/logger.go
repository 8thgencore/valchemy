package logger

import (
	"log/slog"
	"os"
)

// Logger interface for logging
type Logger interface {
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}

// logger implementation of the logger based on slog
type logger struct {
	log *slog.Logger
}

// New creates a new logger with configured formatting and logging level
func New(env string) Logger {
	var log *slog.Logger

	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
		// Add source file and line number to the log
		AddSource: true,
	}

	// In development, use a text format for readability
	// In production, use JSON for machine processing
	if env == "development" {
		log = slog.New(slog.NewTextHandler(os.Stdout, opts))
	} else {
		log = slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}

	// Set the logger as the default logger
	slog.SetDefault(log)

	return &logger{log: log}
}

func (l *logger) Debug(msg string) {
	l.log.Debug(msg)
}

func (l *logger) Info(msg string) {
	l.log.Info(msg)
}

func (l *logger) Warn(msg string) {
	l.log.Warn(msg)
}

func (l *logger) Error(msg string) {
	l.log.Error(msg)
}
