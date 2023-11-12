package bitcask

import (
	"fmt"

	"go.mills.io/bitcask/v2/internal"
)

// Key is a slice of bytes that represents a key in a key/value pair
type Key []byte

// Value is a slice of bytes that represents a value in key/value pair
type Value []byte

// Item is a single key/value pair
type Item struct {
	key   Key
	value Value
}

// Key returns the key
func (i *Item) Key() Key { return i.key }

// Value returns the value
func (i *Item) Value() Value { return i.value }

// KeySize returns the size of the key
func (i *Item) KeySize() int { return len(i.key) }

// ValueSize returns the value of the value
func (i *Item) ValueSize() int { return len(i.value) }

// String implements the fmt.Stringer interface and returns a representation of a key
func (i *Item) String() string { return fmt.Sprintf("key=%q", i.key) }

// KeyFunc is a function that takes a key and performs some operation on it possibly returning an error
type KeyFunc func(Key) error

// Iterator is an interface for iterating over key/value pairs in pre-order
// and seeking to a position in the database by a prefix
type Iterator interface {
	Close() error
	Next() (*Item, error)
	SeekPrefix(Key) (*Item, error)
}

// Batch is an interface for writing ot deleting multiple keys in a batch
// This supports a basic form of "transactions" where one or more keys can be
// written at once in a single operation.
type Batch interface {
	Clear()
	Entries() []internal.Entry
	Delete(Key) (internal.Entry, error)
	Put(Key, Value) (internal.Entry, error)
}

// Transaction is an interface for performing database transactions
type Transaction interface {
	Keys

	Discard()
	Commit() error

	ForEach(KeyFunc) error
	Iterator(...IteratorOption) Iterator
	Range(start Key, end Key, f KeyFunc) error
	Scan(prefix Key, f KeyFunc) error
}

// Keys is an interface for managing database keys
type Keys interface {
	Has(Key) bool
	Get(Key) (Value, error)
	Delete(Key) error
	Put(Key, Value) error
}

// Types is an interface for high-level data types
type Types interface {
	Hash(Key) *Hash
	List(Key) *List
	SortedSet(Key) *SortedSet
}

// DB is an interface that describes the public facing API of a Bitcask database
type DB interface {
	Keys
	Types

	Path() string

	Backup(path string) error
	Stats() (Stats, error)

	Merge() error
	Close() error
	Sync() error

	Len() int

	Batch(...BatchOption) Batch
	Write(Batch) error

	Transaction(...TransactionOption) Transaction

	ForEach(KeyFunc) error
	Iterator(...IteratorOption) Iterator
	Range(start Key, end Key, f KeyFunc) error
	Scan(prefix Key, f KeyFunc) error
}
