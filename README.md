![logo](https://github.com/user-attachments/assets/110d4506-a298-420e-8cb5-3e847a365d4d)

[![Release](https://img.shields.io/github/release/8thgencore/valchemy.svg)](https://github.com/8thgencore/valchemy/releases/latest) 

# Valchemy

Valchemy is a high-performance, distributed key-value storage system with built-in replication support, written in Go.

## Features

- In-memory and on-disk storage engines
- Write-Ahead Logging (WAL) for data durability
- Master-Replica replication support
- Configurable network settings
- Comprehensive logging system
- Interactive CLI client
- Connection pooling with configurable limits

## Getting Started

### Prerequisites

- Go 1.21 or higher
- Task (task runner)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/8thgencore/valchemy.git
cd valchemy
```

2. Install dependencies:
```bash
go mod tidy
```

### Running the Server

1. Start the server with default configuration:
```bash
go run cmd/server/main.go
```

Or with a custom config file:
```bash
go run cmd/server/main.go -config path/to/config.yaml
```

### Running the Client

Connect to a running server:
```bash
go run cmd/cli/main.go
```

Optional flags:
- `-h, --host`: Server host (default: "127.0.0.1")
- `-p, --port`: Server port (default: "3223")

## Configuration

The server can be configured using a YAML configuration file. Key configuration options include:

```yaml
engine:
  type: "in_memory"         # Storage engine type (in_memory/on_disk)

network:
  address: "127.0.0.1:3223" # Client-facing API endpoint
  max_connections: 100      # Maximum concurrent client connections

logging:
  level: "info"            # Log level (debug|info|warn|error)
  output: "stdout"         # Output destination (stdout|file_path)

wal:
  enabled: true            # Enable Write-Ahead Logging
  data_directory: "./data/wal"

replication:
  replica_type: "master"   # Node replication role (master/replica)
  master_host: "127.0.0.1" # Master node API host
  replication_port: "3233" # Port for replica connections
```

## Development

The project uses Task for managing development workflows:

```bash
# Run in development mode with hot reload
task dev

# Run linter
task lint

# Format code
task format

# Run tests
task test

# Run tests with coverage
task test:coverage
```

## Project Structure

```
.
├── cmd/                   # Application entrypoints
│   ├── cli/               # CLI client
│   └── server/            # Server implementation
├── internal/              # Private application code
│   ├── app/               # Application core
│   ├── client/            # Client implementation
│   ├── compute/           # Command processing
│   ├── config/            # Configuration handling
│   ├── replication/       # Replication logic
│   ├── server/            # Server implementation
│   ├── storage/           # Storage engines
│   └── wal/               # Write-Ahead Logging
├── pkg/                   # Public libraries
└── taskfiles/             # Task runner configurations
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
