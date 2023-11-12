package bitcask

import (
	"errors"
	"fmt"
)

var (
	// ErrKeyNotFound is the error returned when a key is not found
	ErrKeyNotFound = errors.New("error: key not found")

	// ErrKeyTooLarge is the error returned for a key that exceeds the
	// maximum allowed key size (configured with WithMaxKeySize).
	ErrKeyTooLarge = errors.New("error: key too large")

	// ErrEmptyKey is the error returned for a value with an empty key.
	ErrEmptyKey = errors.New("error: empty key")

	// ErrValueTooLarge is the error returned for a value that exceeds the
	// maximum allowed value size (configured with WithMaxValueSize).
	ErrValueTooLarge = errors.New("error: value too large")

	// ErrChecksumFailed is the error returned if a key/value retrieved does
	// not match its CRC checksum
	ErrChecksumFailed = errors.New("error: checksum failed")

	// ErrDatabaseLocked is the error returned if the database is locked
	// (typically opened by another process)
	ErrDatabaseLocked = errors.New("error: database locked")

	// ErrDatabaseReadonly is the error returned when the database has been opened in readonly mode
	ErrDatabaseReadonly = errors.New("error: database is readonly")

	// ErrInvalidRange is the error returned when the range scan is invalid
	ErrInvalidRange = errors.New("error: invalid range")

	// ErrInvalidVersion is the error returned when the database version is invalid
	ErrInvalidVersion = errors.New("error: invalid db version")

	// ErrMergeInProgress is the error returned if merge is called when already a merge
	// is in progress
	ErrMergeInProgress = errors.New("error: merge already in progress")
)

// ErrBadConfig is the error returned on failure to load the database config.
type ErrBadConfig struct {
	Err error
}

// Is returns true if the provided target error is the same as ErrBadConfig.
func (e *ErrBadConfig) Is(target error) bool {
	if _, ok := target.(*ErrBadConfig); ok {
		return true
	}
	return errors.Is(e.Err, target)
}

// Unwrap returns the underlying wrapped error that caused ErrBadConfig to be returned.
func (e *ErrBadConfig) Unwrap() error { return e.Err }

// Error implements the error interface.
func (e *ErrBadConfig) Error() string {
	return fmt.Sprintf("error reading config.json: %s", e.Err)
}

// ErrBadMetadata is the error returned on failure to load the database metadata.
type ErrBadMetadata struct {
	Err error
}

// Is returns true if the provided target error is the same as ErrBadConfig.
func (e *ErrBadMetadata) Is(target error) bool {
	if _, ok := target.(*ErrBadConfig); ok {
		return true
	}
	return errors.Is(e.Err, target)
}

// Unwrap returns the underlying wrapped error that caused ErrBadMetadata to be returned.
func (e *ErrBadMetadata) Unwrap() error { return e.Err }

// Error implements the error interface.
func (e *ErrBadMetadata) Error() string {
	return fmt.Sprintf("error reading meta.json: %s", e.Err)
}
