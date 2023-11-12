package bitcask

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.mills.io/bitcask/internal/config"
)

type sortByteArrays [][]byte

func (b sortByteArrays) Len() int {
	return len(b)
}

func (b sortByteArrays) Less(i, j int) bool {
	switch bytes.Compare(b[i], b[j]) {
	case -1:
		return true
	case 0, 1:
		return false
	}
	return false
}

func (b sortByteArrays) Swap(i, j int) {
	b[j], b[i] = b[i], b[j]
}

func SortByteArrays(src [][]byte) [][]byte {
	sorted := sortByteArrays(src)
	sort.Sort(sorted)
	return sorted
}

func TestAll(t *testing.T) {
	var (
		db      DB
		testDir string
		err     error
	)

	testDir, err = os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	t.Run("Open", func(t *testing.T) {
		db, err = Open(testDir)
		assert.NoError(t, err)
	})

	t.Run("Put", func(t *testing.T) {
		err = db.Put(Key("foo"), Value("bar"))
		assert.NoError(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		actual, err := db.Get(Key("foo"))
		assert.NoError(t, err)
		assert.Equal(t, Value("bar"), actual)
	})

	t.Run("Len", func(t *testing.T) {
		assert.Equal(t, 1, db.Len())
	})

	t.Run("Has", func(t *testing.T) {
		assert.True(t, db.Has(Key("foo")))
	})

	t.Run("ForEach", func(t *testing.T) {
		var (
			keys   []Key
			values []Value
		)

		err := db.ForEach(func(key Key) error {
			value, err := db.Get(key)
			if err != nil {
				return err
			}
			keys = append(keys, key)
			values = append(values, value)
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, []Key{[]byte("foo")}, keys)
		assert.Equal(t, []Value{[]byte("bar")}, values)
	})

	t.Run("Delete", func(t *testing.T) {
		err := db.Delete(Key("foo"))
		assert.NoError(t, err)
		_, err = db.Get(Key("foo"))
		assert.Error(t, err)
		assert.Equal(t, ErrKeyNotFound, err)
	})

	t.Run("Sync", func(t *testing.T) {
		assert.NoError(t, db.Sync())
	})

	t.Run("Backup", func(t *testing.T) {
		path, err := os.MkdirTemp("", "backup")
		defer os.RemoveAll(path)
		assert.NoError(t, err)
		err = db.Backup(filepath.Join(path, "db-backup"))
		assert.NoError(t, err)
	})

	t.Run("Close", func(t *testing.T) {
		assert.NoError(t, db.Close())
	})
}

func TestDeletedKeys(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	t.Run("Setup", func(t *testing.T) {
		var (
			db  DB
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})

		t.Run("Put", func(t *testing.T) {
			err = db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(t, err)
		})

		t.Run("Get", func(t *testing.T) {
			actual, err := db.Get(Key("foo"))
			assert.NoError(t, err)
			assert.Equal(t, Value("bar"), actual)
		})

		t.Run("Delete", func(t *testing.T) {
			err := db.Delete([]byte("foo"))
			assert.NoError(t, err)
			_, err = db.Get([]byte("foo"))
			assert.Error(t, err)
			assert.Equal(t, ErrKeyNotFound, err)
		})

		t.Run("Sync", func(t *testing.T) {
			err = db.Sync()
			assert.NoError(t, err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(t, err)
		})
	})

	t.Run("Reopen", func(t *testing.T) {
		var (
			db  DB
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})

		t.Run("Get", func(t *testing.T) {
			_, err = db.Get([]byte("foo"))
			assert.Error(t, err)
			assert.Equal(t, ErrKeyNotFound, err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(t, err)
		})
	})
}

func TestMetadata(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	db, err := Open(testDir)
	require.NoError(t, err)

	require.NoError(t, db.Put([]byte("foo"), []byte("bar")))
	require.NoError(t, db.Close())
	db, err = Open(testDir)
	require.NoError(t, err)

	t.Run("Reclaimable", func(t *testing.T) {
		stats, err := db.Stats()
		require.NoError(t, err)
		assert.Equal(t, int64(0), stats.Reclaimable)
	})
	t.Run("ReclaimableAfterNewPut", func(t *testing.T) {
		stats, err := db.Stats()
		require.NoError(t, err)
		assert.NoError(t, db.Put([]byte("hello"), []byte("world")))
		assert.Equal(t, int64(0), stats.Reclaimable)
	})
	t.Run("ReclaimableAfterRepeatedPut", func(t *testing.T) {
		assert.NoError(t, db.Put(Key("hello"), Value("world")))
		stats, err := db.Stats()
		require.NoError(t, err)
		assert.Equal(t, int64(26), stats.Reclaimable)
	})
	t.Run("ReclaimableAfterDelete", func(t *testing.T) {
		assert.NoError(t, db.Delete([]byte("hello")))
		stats, err := db.Stats()
		require.NoError(t, err)
		assert.Equal(t, int64(73), stats.Reclaimable)
	})
	t.Run("ReclaimableAfterNonExistingDelete", func(t *testing.T) {
		assert.NoError(t, db.Delete([]byte("hello1")))
		stats, err := db.Stats()
		require.NoError(t, err)
		assert.Equal(t, int64(73), stats.Reclaimable)
	})
	t.Run("ReclaimableAfterMerge", func(t *testing.T) {
		assert.NoError(t, db.Merge())
		stats, err := db.Stats()
		require.NoError(t, err)
		assert.Equal(t, int64(0), stats.Reclaimable)
	})
}

func TestConfigErrors(t *testing.T) {
	t.Run("CorruptConfig", func(t *testing.T) {
		testDir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(t, err)
		defer os.RemoveAll(testDir)

		db, err := Open(testDir)
		assert.NoError(t, err)
		assert.NoError(t, db.Close())

		assert.NoError(t, ioutil.WriteFile(filepath.Join(testDir, "config.json"), []byte("foo bar baz"), 0600))

		_, err = Open(testDir)
		assert.Error(t, err)
	})

	t.Run("BadConfigPath", func(t *testing.T) {
		testDir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(t, err)
		defer os.RemoveAll(testDir)

		assert.NoError(t, os.Mkdir(filepath.Join(testDir, "config.json"), 0700))

		_, err = Open(testDir)
		assert.Error(t, err)
	})
}

/* XXX: This test is still broken :/
func TestAutoRecovery(t *testing.T) {
	withAutoRecovery := []bool{false, true}

	for _, autoRecovery := range withAutoRecovery {
		t.Run(fmt.Sprintf("%v", autoRecovery), func(t *testing.T) {
			testDir, err := os.MkdirTemp("", "bitcask")
			require.NoError(t, err)
			db, err := Open(testDir)
			require.NoError(t, err)

			// Insert 10 key-value pairs and verify all is ok.
			makeKeyVal := func(i int) (Key, Value) {
				return Key(fmt.Sprintf("foo%d", i)), Value(fmt.Sprintf("bar%d", i))
			}
			n := 10
			for i := 0; i < n; i++ {
				key, val := makeKeyVal(i)
				err = db.Put(key, val)
				require.NoError(t, err)
			}
			for i := 0; i < n; i++ {
				key, val := makeKeyVal(i)
				actual, err := db.Get(key)
				require.NoError(t, err)
				require.Equal(t, val, actual)
			}
			err = db.Close()
			require.NoError(t, err)

			// Corrupt the last inserted key
			f, err := os.OpenFile(path.Join(testDir, "000000000.data"), os.O_RDWR, 0755)
			require.NoError(t, err)
			fi, err := f.Stat()
			require.NoError(t, err)
			err = f.Truncate(fi.Size() - 1)
			require.NoError(t, err)
			err = f.Close()
			require.NoError(t, err)

			db, err = Open(testDir, WithAutoRecovery(autoRecovery))
			require.NoError(t, err)
			defer db.Close()

			t.Logf("db.Len(): %d", db.Len())

			// Check that all values but the last are still intact.
			for i := 0; i < 9; i++ {
				key, val := makeKeyVal(i)
				t.Logf("key=%s val=%s", key, val)
				actualValue, err := db.Get(key)
				require.NoError(t, err)
				require.Equal(t, val, actualValue)
			}

			// Check the index has no more keys than non-corrupted ones.
			// i.e: all but the last one.
			numKeys := 0
			db.ForEach(func(key Key) error {
				numKeys++
				return nil
			})

			if !autoRecovery {
				// We are opening without auto-repair, and thus are
				// in a corrupted state. The index isn't coherent with
				// the datafile.
				require.Equal(t, n, numKeys)
				return
			}

			require.Equal(t, n-1, numKeys, "The index should have n-1 keys")

			// Double-check explicitly the corrupted one isn't here.
			// This check is redundant considering the last two checks,
			// but doesn't hurt.
			corrKey, _ := makeKeyVal(9)
			_, err = db.Get(corrKey)
			require.Equal(t, ErrKeyNotFound, err)
		})
	}
}
*/

func TestLoadIndexes(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)
	defer os.RemoveAll(testDir)

	var db DB

	t.Run("Setup", func(t *testing.T) {
		db, err = Open(testDir)
		assert.NoError(t, err)
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			val := fmt.Sprintf("val%d", i)
			err := db.Put([]byte(key), []byte(val))
			assert.NoError(t, err)
		}
		err = db.Close()
		assert.NoError(t, err)
	})

	t.Run("OpenAgain", func(t *testing.T) {
		db, err = Open(testDir)
		assert.NoError(t, err)
		assert.Equal(t, 5, db.Len())
	})
}

func TestReIndex(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	t.Run("Setup", func(t *testing.T) {
		var (
			db  DB
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})

		t.Run("Put", func(t *testing.T) {
			err = db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(t, err)
		})

		t.Run("Get", func(t *testing.T) {
			actual, err := db.Get([]byte("foo"))
			assert.NoError(t, err)
			assert.Equal(t, Value("bar"), actual)
		})

		t.Run("Sync", func(t *testing.T) {
			err = db.Sync()
			assert.NoError(t, err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(t, err)
		})

		t.Run("DeleteIndex", func(t *testing.T) {
			err := os.Remove(filepath.Join(testDir, "index"))
			assert.NoError(t, err)
		})
	})

	t.Run("Reopen", func(t *testing.T) {
		var (
			db  DB
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})

		t.Run("Get", func(t *testing.T) {
			actual, err := db.Get([]byte("foo"))
			assert.NoError(t, err)
			assert.Equal(t, Value("bar"), actual)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(t, err)
		})
	})
}

func TestReIndexDeletedKeys(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	t.Run("Setup", func(t *testing.T) {
		var (
			db  DB
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})

		t.Run("Put", func(t *testing.T) {
			err = db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(t, err)
		})

		t.Run("Get", func(t *testing.T) {
			actual, err := db.Get(Key("foo"))
			assert.NoError(t, err)
			assert.Equal(t, Value("bar"), actual)
		})

		t.Run("Delete", func(t *testing.T) {
			err := db.Delete([]byte("foo"))
			assert.NoError(t, err)
			_, err = db.Get(Key("foo"))
			assert.Error(t, err)
			assert.Equal(t, ErrKeyNotFound, err)
		})

		t.Run("Sync", func(t *testing.T) {
			err = db.Sync()
			assert.NoError(t, err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(t, err)
		})

		t.Run("DeleteIndex", func(t *testing.T) {
			err := os.Remove(filepath.Join(testDir, "index"))
			assert.NoError(t, err)
		})
	})

	t.Run("Reopen", func(t *testing.T) {
		var (
			db  DB
			err error
		)

		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})

		t.Run("Get", func(t *testing.T) {
			_, err := db.Get([]byte("foo"))
			assert.Error(t, err)
			assert.Equal(t, ErrKeyNotFound, err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(t, err)
		})
	})
}

func TestSync(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	var db DB

	t.Run("Open", func(t *testing.T) {
		db, err = Open(testDir, WithSync(true))
		assert.NoError(t, err)
	})

	t.Run("Put", func(t *testing.T) {
		key := []byte(strings.Repeat(" ", 17))
		value := []byte("foobar")
		err = db.Put(key, value)
	})

	t.Run("Put", func(t *testing.T) {
		err = db.Put([]byte("hello"), []byte("world"))
		assert.NoError(t, err)
	})
}

func TestMaxKeySize(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	var db DB

	t.Run("Open", func(t *testing.T) {
		db, err = Open(testDir, WithMaxKeySize(16))
		assert.NoError(t, err)
	})

	t.Run("Put", func(t *testing.T) {
		key := []byte(strings.Repeat(" ", 17))
		value := []byte("foobar")
		err = db.Put(key, value)
		assert.Error(t, err)
		assert.Equal(t, ErrKeyTooLarge, err)
	})
}

func TestMaxValueSize(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	var db DB

	t.Run("Open", func(t *testing.T) {
		db, err = Open(testDir, WithMaxValueSize(16))
		assert.NoError(t, err)
	})

	t.Run("Put", func(t *testing.T) {
		key := []byte("foo")
		value := []byte(strings.Repeat(" ", 17))
		err = db.Put(key, value)
		assert.Error(t, err)
		assert.Equal(t, ErrValueTooLarge, err)
	})
}

func TestStats(t *testing.T) {
	var (
		db  DB
		err error
	)

	testDir, err := os.MkdirTemp("", "bitcask")
	require.NoError(t, err)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})

		t.Run("Put", func(t *testing.T) {
			err := db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(t, err)
		})

		t.Run("Get", func(t *testing.T) {
			actual, err := db.Get(Key("foo"))
			assert.NoError(t, err)
			assert.Equal(t, Value("bar"), actual)
		})

		t.Run("Stats", func(t *testing.T) {
			stats, err := db.Stats()
			require.NoError(t, err)

			assert.Equal(t, stats.Datafiles, 0)
			assert.Equal(t, stats.Keys, 1)
		})

		t.Run("Sync", func(t *testing.T) {
			err = db.Sync()
			assert.NoError(t, err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(t, err)
		})
	})
}

func TestStatsError(t *testing.T) {
	var (
		db  DB
		err error
	)

	testDir, err := os.MkdirTemp("", "bitcask")
	require.NoError(t, err)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})

		t.Run("Put", func(t *testing.T) {
			err := db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(t, err)
		})

		t.Run("Get", func(t *testing.T) {
			actual, err := db.Get(Key("foo"))
			assert.NoError(t, err)
			assert.Equal(t, Value("bar"), actual)
		})

		t.Run("Stats", func(t *testing.T) {
			stats, err := db.Stats()
			require.NoError(t, err)

			assert.Equal(t, stats.Datafiles, 0)
			assert.Equal(t, stats.Keys, 1)
		})
	})

	t.Run("Test", func(t *testing.T) {
		t.Run("FabricatedDestruction", func(t *testing.T) {
			// This would never happen in reality :D
			// Or would it? :)
			assert.NoError(t, os.RemoveAll(testDir))
		})

		t.Run("Stats", func(t *testing.T) {
			_, err := db.Stats()
			assert.Error(t, err)
		})
	})
}

func TestDirFileModeBeforeUmask(t *testing.T) {
	t.Run("Setup", func(t *testing.T) {
		t.Run("Default DirFileModeBeforeUmask is 0700", func(t *testing.T) {
			testDir, err := os.MkdirTemp("", "bitcask")
			embeddedDir := filepath.Join(testDir, "cache")
			assert.NoError(t, err)
			defer os.RemoveAll(testDir)

			defaultTestMode := os.FileMode(0700)

			db, err := Open(embeddedDir)
			assert.NoError(t, err)
			defer db.Close()
			err = filepath.Walk(testDir, func(path string, info os.FileInfo, err error) error {
				// skip the root directory
				if path == testDir {
					return nil
				}
				if info.IsDir() {
					// perms for directory on disk are filtered through defaultTestMode, AND umask of user running test.
					// this means the mkdir calls can only FURTHER restrict permissions, not grant more (preventing escalation).
					// to make this test OS agnostic, we'll skip using golang.org/x/sys/unix, inferring umask via XOR and AND NOT.

					// create anotherDir with allPerms - to infer umask
					anotherDir := filepath.Join(testDir, "temp")
					err := os.Mkdir(anotherDir, os.ModePerm)
					assert.NoError(t, err)
					defer os.RemoveAll(anotherDir)

					anotherStat, err := os.Stat(anotherDir)
					assert.NoError(t, err)

					// infer umask from anotherDir
					umask := os.ModePerm ^ (anotherStat.Mode() & os.ModePerm)

					assert.Equal(t, info.Mode()&os.ModePerm, defaultTestMode&^umask)
				}
				return nil
			})
			assert.NoError(t, err)
		})

		t.Run("Dir FileModeBeforeUmask is set via options for all subdirectories", func(t *testing.T) {
			testDir, err := os.MkdirTemp("", "bitcask")
			embeddedDir := filepath.Join(testDir, "cache")
			assert.NoError(t, err)
			defer os.RemoveAll(testDir)

			testMode := os.FileMode(0713)

			db, err := Open(embeddedDir, WithDirMode(testMode))
			assert.NoError(t, err)
			defer db.Close()
			err = filepath.Walk(testDir, func(path string, info os.FileInfo, err error) error {
				// skip the root directory
				if path == testDir {
					return nil
				}
				if info.IsDir() {
					// create anotherDir with allPerms - to infer umask
					anotherDir := filepath.Join(testDir, "temp")
					err := os.Mkdir(anotherDir, os.ModePerm)
					assert.NoError(t, err)
					defer os.RemoveAll(anotherDir)

					anotherStat, _ := os.Stat(anotherDir)

					// infer umask from anotherDir
					umask := os.ModePerm ^ (anotherStat.Mode() & os.ModePerm)

					assert.Equal(t, info.Mode()&os.ModePerm, testMode&^umask)
				}
				return nil
			})
			assert.NoError(t, err)
		})

	})
}

func TestFileFileModeBeforeUmask(t *testing.T) {
	t.Run("Setup", func(t *testing.T) {
		t.Run("Default File FileModeBeforeUmask is 0600", func(t *testing.T) {
			testDir, err := os.MkdirTemp("", "bitcask")
			assert.NoError(t, err)
			defer os.RemoveAll(testDir)

			defaultTestMode := os.FileMode(0600)

			db, err := Open(testDir)
			assert.NoError(t, err)
			defer db.Close()
			err = filepath.Walk(testDir, func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					// the lock file is set within Flock, so ignore it
					if filepath.Base(path) == "lock" {
						return nil
					}
					// create aFile with allPerms - to infer umask
					aFilePath := filepath.Join(testDir, "temp")
					_, err := os.OpenFile(aFilePath, os.O_CREATE, os.ModePerm)
					assert.NoError(t, err)
					defer os.RemoveAll(aFilePath)

					fileStat, _ := os.Stat(aFilePath)

					// infer umask from anotherDir
					umask := os.ModePerm ^ (fileStat.Mode() & os.ModePerm)

					assert.Equal(t, info.Mode()&os.ModePerm, defaultTestMode&^umask)
				}
				return nil
			})
			assert.NoError(t, err)
		})

		t.Run("File FileModeBeforeUmask is set via options for all files", func(t *testing.T) {
			testDir, err := os.MkdirTemp("", "bitcask")
			assert.NoError(t, err)
			defer os.RemoveAll(testDir)

			testMode := os.FileMode(0673)

			db, err := Open(testDir, WithFileMode(testMode))
			assert.NoError(t, err)
			defer db.Close()
			err = filepath.Walk(testDir, func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					// the lock file is set within Flock, so ignore it
					if filepath.Base(path) == "lock" {
						return nil
					}
					// create aFile with allPerms - to infer umask
					aFilePath := filepath.Join(testDir, "temp")
					_, err := os.OpenFile(aFilePath, os.O_CREATE, os.ModePerm)
					assert.NoError(t, err)
					defer os.RemoveAll(aFilePath)

					fileStat, _ := os.Stat(aFilePath)

					// infer umask from anotherDir
					umask := os.ModePerm ^ (fileStat.Mode() & os.ModePerm)

					assert.Equal(t, info.Mode()&os.ModePerm, testMode&^umask)
				}
				return nil
			})
			assert.NoError(t, err)
		})
	})
}

func TestMaxDatafileSize(t *testing.T) {
	var (
		db  DB
		err error
	)

	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)
	defer os.RemoveAll(testDir)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir, WithMaxDatafileSize(32))
			assert.NoError(t, err)
		})

		t.Run("Put", func(t *testing.T) {
			err := db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(t, err)
		})
	})

	t.Run("Put", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			err := db.Put([]byte(fmt.Sprintf("key_%d", i)), []byte("bar"))
			assert.NoError(t, err)
		}
	})

	t.Run("Sync", func(t *testing.T) {
		err = db.Sync()
		assert.NoError(t, err)
	})

	t.Run("Get", func(t *testing.T) {
		actual, err := db.Get(Key("foo"))
		require.NoError(t, err)
		require.Equal(t, Value("bar"), actual)

		for i := 0; i < 10; i++ {
			actual, err = db.Get(Key(fmt.Sprintf("key_%d", i)))
			assert.NoError(t, err)
			assert.Equal(t, Value("bar"), actual)
		}
	})

	t.Run("Close", func(t *testing.T) {
		err = db.Close()
		assert.NoError(t, err)
	})
}

func TestMerge(t *testing.T) {
	var (
		db  DB
		err error
	)

	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir, WithMaxDatafileSize(32))
			assert.NoError(t, err)
		})

		t.Run("Put", func(t *testing.T) {
			err := db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(t, err)
		})

		s1, err := db.Stats()
		assert.NoError(t, err)
		assert.Equal(t, 0, s1.Datafiles)
		assert.Equal(t, 1, s1.Keys)

		t.Run("Put", func(t *testing.T) {
			for i := 0; i < 10; i++ {
				err := db.Put([]byte("foo"), []byte("bar"))
				assert.NoError(t, err)
			}
		})

		s2, err := db.Stats()
		assert.NoError(t, err)
		assert.Equal(t, 5, s2.Datafiles)
		assert.Equal(t, 1, s2.Keys)
		assert.True(t, s2.Size > s1.Size)

		t.Run("Merge", func(t *testing.T) {
			err := db.Merge()
			assert.NoError(t, err)
		})

		s3, err := db.Stats()
		assert.NoError(t, err)
		assert.Equal(t, 2, s3.Datafiles)
		assert.Equal(t, 1, s3.Keys)
		assert.True(t, s3.Size > s1.Size)
		assert.True(t, s3.Size < s2.Size)

		t.Run("Sync", func(t *testing.T) {
			err = db.Sync()
			assert.NoError(t, err)
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(t, err)
		})
	})
}

func TestPutEdgeCases(t *testing.T) {
	t.Run("EmptyValue", func(t *testing.T) {
		testDir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(t, err)

		db, err := Open(testDir)
		assert.NoError(t, err)

		err = db.Put([]byte("alice"), nil)
		assert.NoError(t, err)

		z, err := db.Get([]byte("alice"))
		assert.Error(t, err)
		assert.Equal(t, ErrKeyNotFound, err)
		assert.Nil(t, z)
	})
}

func TestOpenErrors(t *testing.T) {
	t.Run("BadPath", func(t *testing.T) {
		testDir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(t, err)
		defer os.RemoveAll(testDir)

		assert.NoError(t, ioutil.WriteFile(filepath.Join(testDir, "foo"), []byte("foo"), 0600))

		_, err = Open(filepath.Join(testDir, "foo", "tmp.db"))
		assert.Error(t, err)
	})

	t.Run("BadOption", func(t *testing.T) {
		testDir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(t, err)
		defer os.RemoveAll(testDir)

		withBogusOption := func() Option {
			return func(cfg *config.Config) error {
				return errors.New("mocked error")
			}
		}

		_, err = Open(testDir, withBogusOption())
		assert.Error(t, err)
	})

	t.Run("LoadDatafilesError", func(t *testing.T) {
		testDir, err := os.MkdirTemp("", "bitcask")
		assert.NoError(t, err)
		defer os.RemoveAll(testDir)

		db, err := Open(testDir)
		assert.NoError(t, err)

		err = db.Put([]byte("foo"), []byte("bar"))
		assert.NoError(t, err)

		err = db.Close()
		assert.NoError(t, err)

		// Simulate some horrible that happened to the datafiles!
		err = os.Rename(filepath.Join(testDir, "000000000.data"), filepath.Join(testDir, "000000000xxx.data"))
		assert.NoError(t, err)

		_, err = Open(testDir)
		assert.Error(t, err)
		assert.Equal(t, "strconv.ParseInt: parsing \"000000000xxx\": invalid syntax", err.Error())
	})
}

func TestConcurrent(t *testing.T) {
	var (
		db  DB
		err error
	)

	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})

		t.Run("Put", func(t *testing.T) {
			err = db.Put([]byte("foo"), []byte("bar"))
			assert.NoError(t, err)
		})
	})

	t.Run("Concurrent", func(t *testing.T) {
		t.Run("Put", func(t *testing.T) {
			f := func(wg *sync.WaitGroup, x int) {
				defer func() {
					wg.Done()
				}()
				for i := 0; i <= 100; i++ {
					if i%x == 0 {
						key := Key(fmt.Sprintf("k%d", i))
						value := Value(fmt.Sprintf("v%d", i))
						err := db.Put(key, value)
						assert.NoError(t, err)
						actual, err := db.Get(key)
						assert.NoError(t, err)
						assert.Equal(t, value, actual)
					}
				}
			}

			wg := &sync.WaitGroup{}
			wg.Add(3)

			go f(wg, 2)
			go f(wg, 3)
			go f(wg, 5)

			wg.Wait()
		})

		t.Run("Get", func(t *testing.T) {
			f := func(wg *sync.WaitGroup, N int) {
				defer func() {
					wg.Done()
				}()
				for i := 0; i <= N; i++ {
					actual, err := db.Get(Key("foo"))
					assert.NoError(t, err)
					assert.Equal(t, Value("bar"), actual)
				}
			}

			wg := &sync.WaitGroup{}
			wg.Add(3)
			go f(wg, 100)
			go f(wg, 100)
			go f(wg, 100)

			wg.Wait()
		})

		// Test concurrent Put() with concurrent Scan()
		t.Run("PutScan", func(t *testing.T) {
			doPut := func(wg *sync.WaitGroup, x int) {
				defer func() {
					wg.Done()
				}()
				for i := 0; i <= 100; i++ {
					if i%x == 0 {
						key := Key(fmt.Sprintf("k%d", i))
						value := Value(fmt.Sprintf("v%d", i))
						err := db.Put(key, value)
						assert.NoError(t, err)
					}
				}
			}

			doScan := func(wg *sync.WaitGroup, x int) {
				defer func() {
					wg.Done()
				}()
				for i := 0; i <= 100; i++ {
					if i%x == 0 {
						err := db.Scan(Key("k"), func(key Key) error {
							return nil
						})
						assert.NoError(t, err)
					}
				}
			}

			wg := &sync.WaitGroup{}
			wg.Add(6)

			go doPut(wg, 2)
			go doPut(wg, 3)
			go doPut(wg, 5)
			go doScan(wg, 1)
			go doScan(wg, 2)
			go doScan(wg, 4)

			wg.Wait()
		})

		t.Run("ScanMerge", func(t *testing.T) {
			doScan := func(wg *sync.WaitGroup, x int) {
				defer func() {
					wg.Done()
				}()
				for i := 0; i <= 100; i++ {
					if i%x == 0 {
						err := db.Scan(Key("k"), func(key Key) error {
							return nil
						})
						assert.NoError(t, err)
					}
				}
			}

			doMerge := func(wg *sync.WaitGroup, x int) {
				defer func() {
					wg.Done()
				}()
				for i := 0; i <= 100; i++ {
					if i%x == 0 {
						err := db.Merge()
						if err == ErrMergeInProgress {
							continue
						}
						assert.NoError(t, err)
					}
				}
			}

			wg := &sync.WaitGroup{}
			wg.Add(6)

			go doScan(wg, 2)
			go doScan(wg, 3)
			go doScan(wg, 5)
			go doMerge(wg, 1)
			go doMerge(wg, 2)
			go doMerge(wg, 4)

			wg.Wait()
		})

		t.Run("Close", func(t *testing.T) {
			err = db.Close()
			assert.NoError(t, err)
		})
	})
}

func TestScan(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	var db DB

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testDir)
			assert.NoError(t, err)
		})

		t.Run("Put", func(t *testing.T) {
			var items = map[string][]byte{
				"1":     []byte("1"),
				"2":     []byte("2"),
				"3":     []byte("3"),
				"food":  []byte("pizza"),
				"foo":   []byte([]byte("foo")),
				"fooz":  []byte("fooz ball"),
				"hello": []byte("world"),
			}
			for k, v := range items {
				err = db.Put([]byte(k), v)
				assert.NoError(t, err)
			}
		})
	})

	t.Run("Scan", func(t *testing.T) {
		var (
			values   [][]byte
			expected = [][]byte{
				[]byte("foo"),
				[]byte("fooz ball"),
				[]byte("pizza"),
			}
		)

		err = db.Scan([]byte("fo"), func(key Key) error {
			val, err := db.Get(key)
			assert.NoError(t, err)
			values = append(values, val)
			return nil
		})
		values = SortByteArrays(values)
		assert.EqualValues(t, expected, values)
	})
}

func TestRange(t *testing.T) {
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

	t.Run("Range", func(t *testing.T) {
		var (
			values   [][]byte
			expected = [][]byte{
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("7"),
			}
		)

		err = db.Range([]byte("foo_3"), []byte("foo_7"), func(key Key) error {
			val, err := db.Get(key)
			assert.NoError(t, err)
			values = append(values, val)
			return nil
		})
		values = SortByteArrays(values)
		assert.Equal(t, expected, values)
	})

	t.Run("InvalidRange", func(t *testing.T) {
		err = db.Range([]byte("foo_3"), []byte("foo_1"), func(key Key) error {
			return nil
		})
		assert.Error(t, err)
		assert.Equal(t, ErrInvalidRange, err)
	})
}

func TestLocking(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	db, err := Open(testDir)
	assert.NoError(t, err)
	defer db.Close()

	_, err = Open(testDir)
	assert.Error(t, err)
}

func TestLockingAfterMerge(t *testing.T) {
	testDir, err := os.MkdirTemp("", "bitcask")
	assert.NoError(t, err)

	db, err := Open(testDir)
	assert.NoError(t, err)
	defer db.Close()

	_, err = Open(testDir)
	assert.Error(t, err)

	err = db.Merge()
	assert.NoError(t, err)

	// This should still error.
	_, err = Open(testDir)
	assert.Error(t, err)
}

type benchmarkTestCase struct {
	name string
	size int
}

func BenchmarkGet(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	testDir, err := os.MkdirTemp(currentDir, "bitcask_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"512B", 512},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(tt.size))

			key := []byte("foo")
			value := []byte(strings.Repeat(" ", tt.size))

			options := []Option{
				WithMaxKeySize(uint32(len(key))),
				WithMaxValueSize(uint64(tt.size)),
			}
			db, err := Open(testDir, options...)
			if err != nil {
				b.Fatal(err)
			}

			err = db.Put(key, value)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				val, err := db.Get(key)
				if err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(val, value) {
					b.Errorf("unexpected value")
				}
			}
			b.StopTimer()
			db.Close()
		})
	}
}

func BenchmarkPut(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	variants := map[string][]Option{
		"NoSync": {
			WithSync(false),
		},
		"Sync": {
			WithSync(true),
		},
	}

	for name, options := range variants {
		testDir, err := os.MkdirTemp(currentDir, "bitcask_bench")
		if err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(testDir)

		db, err := Open(testDir, options...)
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()

		for _, tt := range tests {
			b.Run(tt.name+name, func(b *testing.B) {
				b.SetBytes(int64(tt.size))
				key := []byte("foo")
				value := []byte(strings.Repeat(" ", tt.size))
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					err := db.Put(key, value)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkScan(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	testDir, err := os.MkdirTemp(currentDir, "bitcask_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	db, err := Open(testDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var items = map[string][]byte{
		"1":     []byte("1"),
		"2":     []byte("2"),
		"3":     []byte("3"),
		"food":  []byte("pizza"),
		"foo":   []byte([]byte("foo")),
		"fooz":  []byte("fooz ball"),
		"hello": []byte("world"),
	}
	for k, v := range items {
		err := db.Put([]byte(k), v)
		if err != nil {
			b.Fatal(err)
		}
	}

	var expected = [][]byte{[]byte("foo"), []byte("food"), []byte("fooz")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var keys [][]byte
		err = db.Scan([]byte("fo"), func(key Key) error {
			keys = append(keys, key)
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
		keys = SortByteArrays(keys)
		if !reflect.DeepEqual(expected, keys) {
			b.Fatal(fmt.Errorf("expected keys=#%v got=%#v", expected, keys))
		}
	}
}
