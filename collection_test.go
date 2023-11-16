package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDB(t *testing.T) DB {
	t.Helper()

	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	db, err := Open(testDir)
	assert.NoError(t, err)

	return db
}

func TestCollection(t *testing.T) {
	type User struct {
		Name string
		Age  int
	}

	t.Run("AddGetDelete", func(t *testing.T) {
		db := setupDB(t)
		defer db.Close()
		c := db.Collection("users")

		err := c.Add("prologic", &User{"James", 21})
		assert.NoError(t, err)

		var actual User
		expected := User{"James", 21}
		require.NoError(t, c.Get("prologic", &actual))
		assert.EqualValues(t, expected, actual)

		actual = User{}
		expected = User{}
		require.NoError(t, c.Delete("prologic"))
		err = c.Get("prologic", &actual)
		require.Error(t, err)
		assert.EqualValues(t, expected, actual)
	})

	t.Run("GetError", func(t *testing.T) {
		db := setupDB(t)
		defer db.Close()
		c := db.Collection("users")

		var actual User
		err := c.Get("foo", &actual)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrObjectNotFound)
		assert.EqualError(t, err, ErrObjectNotFound.Error())
	})

	t.Run("CountExistsEmpty", func(t *testing.T) {
		db := setupDB(t)
		defer db.Close()
		c := db.Collection("users")

		assert.Zero(t, c.Count())
		assert.False(t, c.Exists())
	})

	t.Run("CountNonZero", func(t *testing.T) {
		db := setupDB(t)
		defer db.Close()
		c := db.Collection("users")

		assert.Zero(t, c.Count())
		assert.False(t, c.Exists())

		require.NoError(t, c.Add("prologic", User{"James", 21}))
		assert.Equal(t, 1, c.Count())
		assert.True(t, c.Exists())

	})

	t.Run("Has", func(t *testing.T) {
		db := setupDB(t)
		defer db.Close()
		c := db.Collection("users")

		assert.False(t, c.Has("prologic"))

		require.NoError(t, c.Add("prologic", User{"James", 21}))
		assert.True(t, c.Has("prologic"))
	})

	t.Run("List", func(t *testing.T) {
		db := setupDB(t)
		defer db.Close()
		c := db.Collection("users")

		var (
			actual   []User
			expected []User
		)

		require.NoError(t, c.List(&actual))
		assert.EqualValues(t, expected, actual)

		require.NoError(t, c.Add("prologic", User{"James", 21}))
		require.NoError(t, c.Add("bob", User{"Bob", 99}))     // name is made-up
		require.NoError(t, c.Add("frank", User{"Frank", 37})) // name is made-up

		actual = nil
		expected = []User{
			{"Bob", 99},
			{"Frank", 37},
			{"James", 21},
		}

		require.NoError(t, c.List(&actual))
		assert.Equal(t, expected, actual)
	})
}
