package bitcask

import (
	"errors"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
	"go.mills.io/bitcask/internal"
)

var (
	// ErrIteratorClosed ...
	ErrIteratorClosed = errors.New("error: iterator is closed")

	// ErrStopIteration ...
	ErrStopIteration = errors.New("error: iterator has no more items")
)

// IteratorOptions ...
type IteratorOptions struct {
	Reverse bool
}

// IteratorOption ...
type IteratorOption func(it *iterator)

// Reverse ...
func Reverse() IteratorOption {
	return func(it *iterator) {
		it.opts.Reverse = true
	}
}

type iterator struct {
	keys Keys
	itf  *iradix.Iterator[internal.Item]
	itr  *iradix.ReverseIterator[internal.Item]
	opts *IteratorOptions
}

func (it *iterator) Close() error {
	if it.itf == nil && it.itr == nil {
		return ErrIteratorClosed
	}
	it.itf = nil
	it.itr = nil
	return nil
}

func (it *iterator) Next() (*Item, error) {
	var (
		key  []byte
		more bool
	)

	if it.opts.Reverse {
		key, _, more = it.itr.Previous()
	} else {
		key, _, more = it.itf.Next()
	}

	if !more {
		defer it.Close()
		return nil, ErrStopIteration
	}
	value, err := it.keys.Get(key)
	if err != nil {
		defer it.Close()
		return nil, err
	}
	return &Item{key, value}, nil
}

func (it *iterator) SeekPrefix(prefix Key) (*Item, error) {
	if it.opts.Reverse {
		it.itr.SeekPrefix(prefix)
	} else {
		it.itf.SeekPrefix(prefix)
	}
	return it.Next()
}

// Iterator returns an iterator for iterating through keys in key order
func (b *bitcask) Iterator(opts ...IteratorOption) Iterator {
	it := &iterator{keys: b, opts: &IteratorOptions{}}
	for _, opt := range opts {
		opt(it)
	}
	if it.opts.Reverse {
		it.itr = b.trie.Root().ReverseIterator()
	} else {
		it.itf = b.trie.Root().Iterator()
	}
	return it
}
