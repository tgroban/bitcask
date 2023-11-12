// Package config defines configuration details and functions to load and save configuration to disk
package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// Config contains the bitcask configuration parameters
type Config struct {
	MaxDatafileSize int         `json:"max_datafile_size"`
	MaxKeySize      uint32      `json:"max_key_size"`
	MaxValueSize    uint64      `json:"max_value_size"`
	SyncWrites      bool        `json:"sync_writes"`
	AutoReadonly    bool        `json:"auto_readonly"`
	AutoRecovery    bool        `json:"auto_recovery"`
	DirMode         os.FileMode `json:"dir_mode"`
	FileMode        os.FileMode `json:"file_mode"`
}

// Load loads a configuration from the given path
func Load(path string) (*Config, error) {
	var cfg Config

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Save saves the configuration to the provided path
func (c *Config) Save(path string) error {

	data, err := json.Marshal(c)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path, data, c.FileMode)
	if err != nil {
		return err
	}

	return nil
}
