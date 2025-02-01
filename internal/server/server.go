package server

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/8thgencore/valchemy/internal/compute"
	"github.com/8thgencore/valchemy/internal/config"
	"github.com/8thgencore/valchemy/pkg/constants"
	"github.com/8thgencore/valchemy/pkg/logger/sl"
)

// Server is the server struct
type Server struct {
	log           *slog.Logger
	config        *config.NetworkConfig
	handler       *compute.Handler
	listener      net.Listener
	connections   sync.WaitGroup
	connCount     int32
	connCountLock sync.Mutex
}

// NewServer creates a new server
func NewServer(log *slog.Logger, config *config.NetworkConfig, handler *compute.Handler) *Server {
	return &Server{
		log:     log,
		config:  config,
		handler: handler,
	}
}

// Start starts the server
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	s.listener = listener
	s.log.Info("Server started", "address", s.config.Address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.log.Error("Failed to accept connection", sl.Err(err))
			continue
		}

		if !s.canAcceptConnection() {
			s.log.Warn("Max connections reached, rejecting connection")
			err := conn.Close()
			if err != nil {
				s.log.Error("Failed to close connection", sl.Err(err))
			}

			continue
		}

		s.connections.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection handles a connection
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			s.log.Error("Failed to close connection", sl.Err(err))
		}
		s.connections.Done()
		s.decrementConnCount()
	}()

	s.log.Info("New connection established", "remote_addr", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				s.log.Info("Client disconnected", "remote_addr", conn.RemoteAddr())
				return
			}
			s.log.Error("Failed to read from connection", sl.Err(err))

			return
		}

		response, err := s.handler.Handle(strings.TrimSpace(input))
		if err != nil {
			response = fmt.Sprintf("ERROR: %s", err)
		}

		// Add a special end message marker
		response = response + "\n" + constants.EndMarker

		if _, err := conn.Write([]byte(response)); err != nil {
			s.log.Error("Failed to write response", sl.Err(err))
			return
		}
	}
}

// canAcceptConnection checks if the server can accept a new connection
func (s *Server) canAcceptConnection() bool {
	s.connCountLock.Lock()
	defer s.connCountLock.Unlock()

	if int(s.connCount) >= s.config.MaxConnections {
		return false
	}

	s.connCount++

	return true
}

// decrementConnCount decrements the connection count
func (s *Server) decrementConnCount() {
	s.connCountLock.Lock()
	s.connCount--
	s.connCountLock.Unlock()
}
