package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortedSet(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	require.NoError(t, err)

	var (
		db DB
		z  *SortedSet
	)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
			z = db.SortedSet([]byte("foo"))
		})
	})

	t.Run("Add", func(t *testing.T) {
		added, err := z.Add(
			Int64ToScore(1), []byte("a"),
			Int64ToScore(2), []byte("b"),
			Int64ToScore(3), []byte("c"),
		)
		assert.NoError(t, err)
		assert.Equal(t, 3, added)
	})

	t.Run("Score", func(t *testing.T) {
		score, err := z.Score([]byte("b"))
		assert.NoError(t, err)
		assert.Equal(t, int64(2), ScoreToInt64(score))
	})

	t.Run("Range", func(t *testing.T) {
		var (
			actual   []Key
			expected = []Key{
				Key("a"),
				Key("b"),
				Key("c"),
			}
		)

		err := z.Range(Int64ToScore(1), Int64ToScore(3), func(i int64, score Score, member []byte, stop *bool) {
			actual = append(actual, member)
		})
		require.NoError(t, err)
		assert.EqualValues(t, expected, actual)
	})
}
