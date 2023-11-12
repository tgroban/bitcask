// Package data implements on disk and in memory storage for data files
package data

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/mattetti/filebuffer"
	"github.com/pkg/errors"
	"golang.org/x/exp/mmap"

	"go.mills.io/bitcask/v2/internal"
	"go.mills.io/bitcask/v2/internal/codec"
)

const (
	defaultDatafileFilename = "%09d.data"
)

var (
	errReadonly  = errors.New("error: read only datafile")
	errReadError = errors.New("error: read error")
)

// Datafile is an interface  that represents a readable and writeable datafile
type Datafile interface {
	FileID() int
	Name() string
	Close() error
	Sync() error
	Size() int64
	Read() (internal.Entry, int64, error)
	ReadAt(index, size int64) (internal.Entry, error)
	Write(internal.Entry) (int64, int64, error)
	Readonly() Datafile
}

// NewOnDiskDatafile opens an existing on disk datafile
func NewOnDiskDatafile(path string, id int, readonly bool, maxKeySize uint32, maxValueSize uint64, fileMode os.FileMode) (Datafile, error) {
	var (
		r   *os.File
		ra  *mmap.ReaderAt
		w   *os.File
		err error
	)

	fn := filepath.Join(path, fmt.Sprintf(defaultDatafileFilename, id))

	if !readonly {
		w, err = os.OpenFile(fn, os.O_WRONLY|os.O_APPEND|os.O_CREATE, fileMode)
		if err != nil {
			return nil, err
		}
	}

	r, err = os.Open(fn)
	if err != nil {
		return nil, err
	}
	stat, err := r.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "error calling Stat()")
	}

	if readonly {
		ra, err = mmap.Open(fn)
		if err != nil {
			return nil, err
		}
	}

	offset := stat.Size()

	dec := codec.NewDecoder(r, maxKeySize, maxValueSize)
	enc := codec.NewEncoder(w)

	return &onDiskDatafile{
		id:           id,
		r:            r,
		ra:           ra,
		w:            w,
		offset:       offset,
		dec:          dec,
		enc:          enc,
		maxKeySize:   maxKeySize,
		maxValueSize: maxValueSize,
	}, nil
}

// NewInMemoryDatafile creates a new in-memory datafile
func NewInMemoryDatafile(id int, maxKeySize uint32, maxValueSize uint64) Datafile {
	buf := filebuffer.New(nil)

	dec := codec.NewDecoder(buf, maxKeySize, maxValueSize)
	enc := codec.NewEncoder(buf)

	return &inMemoryDatafile{
		id:           id,
		buf:          buf,
		dec:          dec,
		enc:          enc,
		maxKeySize:   maxKeySize,
		maxValueSize: maxValueSize,
	}
}
