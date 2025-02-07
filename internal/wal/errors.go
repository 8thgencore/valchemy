package wal

import "errors"

var (
	// ErrWALClosed returned when trying to operate on closed WAL
	ErrWALClosed = errors.New("WAL already closed")

	// ErrFlushWAL returned when batch flush fails
	ErrFlushWAL = errors.New("failed to flush WAL")

	// ErrWriteEntry returned when entry write fails
	ErrWriteEntry = errors.New("failed to write entry")

	// ErrSyncWAL returned when WAL sync fails
	ErrSyncWAL = errors.New("failed to sync WAL")

	// ErrFlushFinalBatch returned when flushing final batch on close fails
	ErrFlushFinalBatch = errors.New("failed to flush final batch")

	// ErrCreateSegment returned when segment creation fails
	ErrCreateSegment = errors.New("failed to create segment")

	// ErrRotateSegment returned when segment rotation fails
	ErrRotateSegment = errors.New("failed to rotate segment")

	// ErrCloseSegment returned when closing current segment fails
	ErrCloseSegment = errors.New("failed to close current segment")
)
