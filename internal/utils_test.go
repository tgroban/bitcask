package internal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SaveAndLoad(t *testing.T) {
	t.Run("save and load", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(t, err)
		defer os.RemoveAll(tempDir)
		type test struct {
			Value bool `json:"value"`
		}
		m := test{Value: true}
		err = SaveJSONToFile(&m, filepath.Join(tempDir, "meta.json"), os.FileMode(0644))
		assert.NoError(t, err)
		m1 := test{}
		err = LoadFromJSONFile(filepath.Join(tempDir, "meta.json"), &m1)
		assert.NoError(t, err)
		assert.Equal(t, m, m1)
	})

	t.Run("save and load error", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(t, err)
		defer os.RemoveAll(tempDir)
		type test struct {
			Value bool `json:"value"`
		}
		err = SaveJSONToFile(make(chan int), filepath.Join(tempDir, "meta.json"), os.FileMode(644))
		assert.Error(t, err)
		m1 := test{}
		err = LoadFromJSONFile(filepath.Join(tempDir, "meta.json"), &m1)
		assert.Error(t, err)
	})
}
