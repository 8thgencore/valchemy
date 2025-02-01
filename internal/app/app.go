package app

import (
	"fmt"
	"log/slog"

	"github.com/8thgencore/valchemy/internal/compute"
	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/internal/server"
	"github.com/8thgencore/valchemy/internal/storage"
	"github.com/8thgencore/valchemy/internal/wal"
	"github.com/8thgencore/valchemy/pkg/logger"
)

// App represents the main application
type App struct {
	cfg    *config.Config
	log    *slog.Logger
	server *server.Server
}

// New creates a new instance of the application
func New(configPath string) (*App, error) {
	// Load configuration
	cfg, err := config.NewConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Initialize logger
	log := logger.New(cfg.Env)

	// Initialize WAL
	wal, err := wal.New(cfg.WAL)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// Initialize storage engine
	engine := storage.NewEngine(wal)

	// Initialize command handler
	handler := compute.NewHandler(log, engine)

	// Initialize server
	srv := server.NewServer(log, &cfg.Network, handler)

	return &App{
		cfg:    cfg,
		log:    log,
		server: srv,
	}, nil
}

// Run starts the application
func (a *App) Run() error {
	a.log.Info("Starting application", "env", a.cfg.Env)
	return a.server.Start()
}
