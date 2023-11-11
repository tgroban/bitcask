package internal

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Copy(t *testing.T) {
	t.Run("CopyDir", func(t *testing.T) {
		tempSrc, err := os.MkdirTemp("", "test")
		assert.NoError(t, err)
		defer os.RemoveAll(tempSrc)
		var f *os.File

		tempDir, err := os.MkdirTemp(tempSrc, "")
		assert.NoError(t, err)

		f, err = os.OpenFile(filepath.Join(tempSrc, "file1"), os.O_WRONLY|os.O_CREATE, os.FileMode(644))
		assert.NoError(t, err)
		n, err := f.WriteString("test123")
		assert.Equal(t, 7, n)
		assert.NoError(t, err)
		f.Close()

		f, err = os.OpenFile(filepath.Join(tempSrc, "file2"), os.O_WRONLY|os.O_CREATE, os.FileMode(644))
		assert.NoError(t, err)
		n, err = f.WriteString("test1234")
		assert.Equal(t, 8, n)
		assert.NoError(t, err)
		f.Close()

		f, err = os.OpenFile(filepath.Join(tempSrc, "file3"), os.O_WRONLY|os.O_CREATE, os.FileMode(644))
		assert.NoError(t, err)
		f.Close()

		tempDest, err := os.MkdirTemp("", "backup")
		assert.NoError(t, err)
		defer os.RemoveAll(tempDest)
		err = Copy(tempSrc, tempDest, []string{"file3"})
		assert.NoError(t, err)
		buf := make([]byte, 10)

		exists := Exists(filepath.Join(tempDest, filepath.Base(tempDir)))
		assert.Equal(t, true, exists)

		f, err = os.Open(filepath.Join(tempDest, "file1"))
		assert.NoError(t, err)
		n, err = f.Read(buf[:7])
		assert.NoError(t, err)
		assert.Equal(t, 7, n)
		assert.Equal(t, []byte("test123"), buf[:7])
		_, err = f.Read(buf)
		assert.Equal(t, io.EOF, err)
		f.Close()

		f, err = os.Open(filepath.Join(tempDest, "file2"))
		assert.NoError(t, err)
		n, err = f.Read(buf[:8])
		assert.NoError(t, err)
		assert.Equal(t, 8, n)
		assert.Equal(t, []byte("test1234"), buf[:8])
		_, err = f.Read(buf)
		assert.Equal(t, io.EOF, err)
		f.Close()

		exists = Exists(filepath.Join(tempDest, "file3"))
		assert.Equal(t, false, exists)
	})
}

func Test_SaveAndLoad(t *testing.T) {
	t.Run("save and load", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(t, err)
		defer os.RemoveAll(tempDir)
		type test struct {
			Value bool `json:"value"`
		}
		m := test{Value: true}
		err = SaveJSONToFile(&m, filepath.Join(tempDir, "meta.json"), os.FileMode(644))
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
