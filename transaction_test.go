package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransaction(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	var db DB

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})
	})

	t.Run("Transaction", func(t *testing.T) {
		tx := db.Transaction()
		defer tx.Discard()

		tx.Put(Key("foo"), Value("bar"))
		tx.Put(Key("hello"), Value("world"))

		tests := map[string]Value{
			"foo":   Value("bar"),
			"hello": Value("world"),
		}

		// test that keys are in the transaction
		for key, val := range tests {
			actual, err := tx.Get(Key(key))
			assert.NoError(t, err)
			assert.Equal(t, val, actual)
		}

		assert.NoError(t, err, tx.Commit())

		// and committed correctly
		for key, val := range tests {
			actual, err := db.Get(Key(key))
			assert.NoError(t, err)
			assert.Equal(t, val, actual)
		}

	})
}
