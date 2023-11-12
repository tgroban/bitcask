package bitcask

import (
	"os"

	"go.mills.io/bitcask/v2/internal/config"
)

const (
	// DefaultDirMode is the default os.FileMode used when creating directories
	DefaultDirMode = os.FileMode(0700)

	// DefaultFileMode is the default os.FileMode used when creating files
	DefaultFileMode = os.FileMode(0600)

	// DefaultMaxDatafileSize is the default maximum datafile size in bytes
	DefaultMaxDatafileSize = 1 << 20 // 1MB

	// DefaultMaxKeySize is the default maximum key size in bytes
	DefaultMaxKeySize = uint32(64) // 64 bytes

	// DefaultMaxValueSize is the default value size in bytes
	DefaultMaxValueSize = uint64(1 << 16) // 65KB

	// DefaultSync is the default file synchronization action
	DefaultSync = false

	// DefaultAutoRecovery is the default auto-recovery action.
	DefaultAutoRecovery = true
)

// Option is a function that takes a config struct and modifies it
type Option func(*config.Config) error

func withConfig(src *config.Config) Option {
	return func(cfg *config.Config) error {
		cfg.MaxDatafileSize = src.MaxDatafileSize
		cfg.MaxKeySize = src.MaxKeySize
		cfg.MaxValueSize = src.MaxValueSize
		cfg.Sync = src.Sync
		cfg.AutoRecovery = src.AutoRecovery
		cfg.DirMode = src.DirMode
		cfg.FileMode = src.FileMode
		return nil
	}
}

// WithAutoRecovery sets auto recovery of data and index file recreation.
// IMPORTANT: This flag MUST BE used only if a proper backup was made of all
// the existing datafiles.
func WithAutoRecovery(enabled bool) Option {
	return func(cfg *config.Config) error {
		cfg.AutoRecovery = enabled
		return nil
	}
}

// WithDirMode sets the FileMode used for each new file created.
func WithDirMode(mode os.FileMode) Option {
	return func(cfg *config.Config) error {
		cfg.DirMode = mode
		return nil
	}
}

// WithFileMode sets the FileMode used for each new file created.
func WithFileMode(mode os.FileMode) Option {
	return func(cfg *config.Config) error {
		cfg.FileMode = mode
		return nil
	}
}

// WithMaxDatafileSize sets the maximum datafile size option
func WithMaxDatafileSize(size int) Option {
	return func(cfg *config.Config) error {
		cfg.MaxDatafileSize = size
		return nil
	}
}

// WithMaxKeySize sets the maximum key size option
func WithMaxKeySize(size uint32) Option {
	return func(cfg *config.Config) error {
		cfg.MaxKeySize = size
		return nil
	}
}

// WithMaxValueSize sets the maximum value size option
func WithMaxValueSize(size uint64) Option {
	return func(cfg *config.Config) error {
		cfg.MaxValueSize = size
		return nil
	}
}

// WithSync causes Sync() to be called on every key/value written increasing
// durability and safety at the expense of performance
func WithSync(sync bool) Option {
	return func(cfg *config.Config) error {
		cfg.Sync = sync
		return nil
	}
}

func newDefaultConfig() *config.Config {
	return &config.Config{
		MaxDatafileSize: DefaultMaxDatafileSize,
		MaxKeySize:      DefaultMaxKeySize,
		MaxValueSize:    DefaultMaxValueSize,
		Sync:            DefaultSync,
		DirMode:         DefaultDirMode,
		FileMode:        DefaultFileMode,
	}
}
