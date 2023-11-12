// Package index deals with reading and writing database indexes
package index

import (
	"os"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
	"go.mills.io/bitcask/v2/internal"
)

// Indexer is an interface for loading and saving the index (an Adaptive Radix Tree)
type Indexer[T any] interface {
	Load(path string, maxKeySize uint32) (*iradix.Tree[T], error)
	Save(t *iradix.Tree[T], path string) error
}

// NewIndexer returns an instance of the default `Indexer` implementation
// which persists the index (an Adaptive Radix Tree) as a binary blob on file
func NewIndexer() Indexer[internal.Item] {
	return &indexer{}
}

type indexer struct{}

func (i *indexer) Load(path string, maxKeySize uint32) (*iradix.Tree[internal.Item], error) {
	t := iradix.New[internal.Item]()

	f, err := os.Open(path)
	if err != nil {
		return t, err
	}
	defer f.Close()

	t, err = readIndex(f, t, maxKeySize)
	if err != nil {
		return t, err
	}
	return t, nil
}

func (i *indexer) Save(t *iradix.Tree[internal.Item], path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := writeIndex(t, f); err != nil {
		return err
	}

	return f.Close()
}
