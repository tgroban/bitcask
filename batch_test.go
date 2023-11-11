package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	var db DB

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})
	})

	t.Run("Batch", func(t *testing.T) {
		b := db.Batch()
		b.Put(Key("foo"), Value("bar"))
		b.Put(Key("hello"), Value("world"))

		assert.NoError(t, err, db.Write(b))

		tests := map[string]Value{
			"foo":   Value("bar"),
			"hello": Value("world"),
		}

		for key, val := range tests {
			actual, err := db.Get(Key(key))
			assert.NoError(t, err)
			assert.Equal(t, val, actual)
		}
	})
}
