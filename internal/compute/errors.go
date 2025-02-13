package compute

import "errors"

// ErrKeyNotFound is an error that occurs when the key is not found
var ErrKeyNotFound = errors.New("key not found")

// ErrInvalidFormat is an error that occurs when the command format is invalid
var ErrInvalidFormat = errors.New("invalid command format")

// ErrUnknownCommand is an error that occurs when the command is unknown
var ErrUnknownCommand = errors.New("unknown command")

// ErrInvalidSetFormat is an error that occurs when the SET command format is invalid
var ErrInvalidSetFormat = errors.New("invalid SET command format")

// ErrReadOnlyReplica is an error that occurs when the replica is read-only
var ErrReadOnlyReplica = errors.New("replica is read-only: only GET and HELP commands are allowed")
