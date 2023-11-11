package bitcask

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIterator(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	var db DB

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})

		t.Run("Put", func(t *testing.T) {
			for i := 1; i < 10; i++ {
				key := []byte(fmt.Sprintf("foo_%d", i))
				val := []byte(fmt.Sprintf("%d", i))
				err = db.Put(key, val)
				assert.NoError(t, err)
			}
		})
	})

	t.Run("IteratorForward", func(t *testing.T) {
		var (
			values   [][]byte
			expected = [][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("7"),
				[]byte("8"),
				[]byte("9"),
			}
		)

		it := db.Iterator()
		defer it.Close()
		for {
			item, err := it.Next()
			if err != nil || err == ErrStopIteration {
				break
			}
			values = append(values, item.Value())
		}
		assert.EqualValues(t, expected, values)
	})

	t.Run("IteratorReverse", func(t *testing.T) {
		var (
			values   [][]byte
			expected = [][]byte{
				[]byte("9"),
				[]byte("8"),
				[]byte("7"),
				[]byte("6"),
				[]byte("5"),
				[]byte("4"),
				[]byte("3"),
				[]byte("2"),
				[]byte("1"),
			}
		)

		it := db.Iterator(Reverse())
		defer it.Close()
		for {
			item, err := it.Next()
			if err != nil || err == ErrStopIteration {
				break
			}
			values = append(values, item.Value())
		}
		assert.EqualValues(t, expected, values)
	})
}
