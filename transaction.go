package bitcask

import (
	"bytes"
	"hash/crc32"
	"log"

	"github.com/abcum/lcp"
	iradix "github.com/hashicorp/go-immutable-radix/v2"
	"go.mills.io/bitcask/v2/internal"
	"go.mills.io/bitcask/v2/internal/config"
	"go.mills.io/bitcask/v2/internal/data"
)

type transactionOptions struct{}

func defaultTransactionOptions(cfg *config.Config) *transactionOptions {
	return &transactionOptions{}
}

// TransactionOption ...
type TransactionOption func(t *transaction)

type transaction struct {
	db        DB
	current   data.Datafile
	previous  data.Datafile
	datafiles map[int]data.Datafile
	batch     Batch
	trie      *iradix.Txn[internal.Item]
	opts      *transactionOptions
}

func (t *transaction) Discard() {}

func (t *transaction) Commit() error {
	return t.db.Write(t.batch)
}

func (t *transaction) Has(key Key) bool {
	_, found := t.trie.Root().Get(key)
	return found
}

func (t *transaction) Get(key Key) (Value, error) {
	e, err := t.get(key)
	if err != nil {
		return nil, err
	}
	return e.Value, nil
}

func (t *transaction) get(key []byte) (internal.Entry, error) {
	var df data.Datafile

	item, found := t.trie.Root().Get(key)

	if !found {
		return internal.Entry{}, ErrKeyNotFound
	}

	switch item.FileID {
	case t.current.FileID():
		df = t.current
	case t.previous.FileID():
		df = t.previous
	default:
		df = t.datafiles[item.FileID]
	}

	e, err := df.ReadAt(item.Offset, item.Size)
	if err != nil {
		return internal.Entry{}, err
	}

	checksum := crc32.ChecksumIEEE(e.Value)
	if checksum != e.Checksum {
		return internal.Entry{}, ErrChecksumFailed
	}

	return e, nil
}

func (t *transaction) Delete(key Key) error {
	entry, err := t.batch.Delete(key)
	if err != nil {
		return err
	}

	_, _, err = t.current.Write(entry)
	if err != nil {
		return err
	}

	_, _ = t.trie.Delete(key)

	return nil
}

func (t *transaction) Put(key Key, value Value) error {
	entry, err := t.batch.Put(key, value)
	if err != nil {
		return err
	}

	offset, n, err := t.current.Write(entry)
	if err != nil {
		return err
	}

	item := internal.Item{FileID: t.current.FileID(), Offset: offset, Size: n}

	_, _ = t.trie.Insert(key, item)

	return nil
}

func (t *transaction) ForEach(f KeyFunc) (err error) {
	t.trie.Root().Walk(func(key []byte, item internal.Item) bool {
		if err = f(key); err != nil {
			return true
		}
		return false
	})

	return
}

func (t *transaction) Iterator(opts ...IteratorOption) Iterator {
	it := &iterator{keys: t, opts: &iteratorOptions{}}
	for _, opt := range opts {
		opt(it)
	}
	if it.opts.reverse {
		it.itr = t.trie.Root().ReverseIterator()
	} else {
		it.itf = t.trie.Root().Iterator()
	}
	return it
}

func (t *transaction) Range(start Key, end Key, f KeyFunc) (err error) {
	if bytes.Compare(start, end) == 1 {
		return ErrInvalidRange
	}

	commonPrefix := lcp.LCP(start, end)
	if commonPrefix == nil {
		return ErrInvalidRange
	}

	log.Printf("commonPrefix: %q\n", commonPrefix)

	t.trie.Root().WalkPrefix(commonPrefix, func(key []byte, item internal.Item) bool {
		if bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) <= 0 {
			if err = f(key); err != nil {
				return true
			}
			return false
		} else if bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) > 0 {
			return true
		}
		return false
	})
	return
}

func (t *transaction) Scan(prefix Key, f KeyFunc) (err error) {
	t.trie.Root().WalkPrefix(prefix, func(key []byte, item internal.Item) bool {
		// Skip the root node
		if len(key) == 0 {
			return false
		}

		if err = f(key); err != nil {
			return true
		}
		return false
	})
	return
}

func (b *bitcask) Transaction(opts ...TransactionOption) Transaction {
	b.mu.RLock()
	defer b.mu.RUnlock()

	current := data.NewInMemoryDatafile(-1, b.config.MaxKeySize, b.config.MaxValueSize)
	previous := b.current.Readonly()
	datafiles := b.datafiles

	txn := &transaction{
		db:        b,
		current:   current,
		previous:  previous,
		datafiles: datafiles,
		batch:     b.Batch(),
		trie:      b.trie.Txn(),
		opts:      defaultTransactionOptions(b.config),
	}

	for _, opt := range opts {
		opt(txn)
	}

	return txn
}
