package bitcask

import (
	"bytes"
	"hash/crc32"
	"log"

	"github.com/abcum/lcp"
	iradix "github.com/hashicorp/go-immutable-radix/v2"
	"go.mills.io/bitcask/internal"
	"go.mills.io/bitcask/internal/config"
	"go.mills.io/bitcask/internal/data"
)

type transactionOptions struct {
	maxKeySize   uint32
	maxValueSize uint64
}

func defaultTransactionOptions(cfg *config.Config) *transactionOptions {
	return &transactionOptions{
		maxKeySize:   cfg.MaxKeySize,
		maxValueSize: cfg.MaxValueSize,
	}
}

// TransactionOption ...
type TransactionOption func(t *transaction)

// WithTransactionMaxKeySize sets the maximum key size option
func WithTransactionMaxKeySize(size uint32) TransactionOption {
	return func(t *transaction) {
		t.opts.maxKeySize = size
	}
}

// WithTransactionMaxValueSize sets the maximum value size option
func WithTransactionMaxValueSize(size uint64) TransactionOption {
	return func(t *transaction) {
		t.opts.maxValueSize = size
	}
}

type transaction struct {
	_db       *bitcask
	db        DB
	curr      data.Datafile
	prev      data.Datafile
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
	case t.curr.FileID():
		df = t.curr
	case t.prev.FileID():
		df = t.prev
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

	_, _, err = t.curr.Write(entry)
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

	offset, n, err := t.curr.Write(entry)
	if err != nil {
		return err
	}

	item := internal.Item{FileID: t.curr.FileID(), Offset: offset, Size: n}

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
	it := &iterator{keys: t, opts: &IteratorOptions{}}
	for _, opt := range opts {
		opt(it)
	}
	if it.opts.Reverse {
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

	curr := data.NewInMemoryDatafile(-1, b.config.MaxKeySize, b.config.MaxValueSize)

	prev := b.curr.Readonly()

	datafiles := b.datafiles

	txn := &transaction{
		_db:       b,
		db:        b,
		curr:      curr,
		prev:      prev,
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
