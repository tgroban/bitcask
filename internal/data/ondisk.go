package data

import (
	"os"
	"sync"

	"golang.org/x/exp/mmap"

	"go.mills.io/bitcask/v2/internal"
	"go.mills.io/bitcask/v2/internal/codec"
)

type onDiskDatafile struct {
	sync.RWMutex

	id           int
	r            *os.File
	ra           *mmap.ReaderAt
	w            *os.File
	offset       int64
	dec          *codec.Decoder
	enc          *codec.Encoder
	maxKeySize   uint32
	maxValueSize uint64
}

func (df *onDiskDatafile) FileID() int {
	return df.id
}

func (df *onDiskDatafile) Name() string {
	return df.r.Name()
}

func (df *onDiskDatafile) Close() error {
	defer func() {
		if df.ra != nil {
			df.ra.Close()
		}
		df.r.Close()
	}()

	// Readonly datafile -- Nothing further to close on the write side
	if df.w == nil {
		return nil
	}

	err := df.Sync()
	if err != nil {
		return err
	}
	return df.w.Close()
}

func (df *onDiskDatafile) Sync() error {
	if df.w == nil {
		return nil
	}
	return df.w.Sync()
}

func (df *onDiskDatafile) Size() int64 {
	df.RLock()
	defer df.RUnlock()
	return df.offset
}

// Read reads the next entry from the datafile
func (df *onDiskDatafile) Read() (e internal.Entry, n int64, err error) {
	df.Lock()
	defer df.Unlock()

	n, err = df.dec.Decode(&e)
	if err != nil {
		return
	}

	return
}

// ReadAt the entry located at index offset with expected serialized size
func (df *onDiskDatafile) ReadAt(index, size int64) (e internal.Entry, err error) {
	var n int

	b := make([]byte, size)

	df.RLock()
	defer df.RUnlock()

	if df.ra != nil {
		n, err = df.ra.ReadAt(b, index)
	} else {
		n, err = df.r.ReadAt(b, index)
	}
	if err != nil {
		return
	}
	if int64(n) != size {
		err = errReadError
		return
	}

	codec.DecodeEntry(b, &e, df.maxKeySize, df.maxValueSize)

	return
}

func (df *onDiskDatafile) Write(e internal.Entry) (int64, int64, error) {
	if df.w == nil {
		return -1, 0, errReadonly
	}

	df.Lock()
	defer df.Unlock()

	offset := df.offset

	n, err := df.enc.Encode(e)
	if err != nil {
		return -1, 0, err
	}
	df.offset += n

	return offset, n, nil
}

func (df *onDiskDatafile) Readonly() Datafile {
	df.RLock()
	defer df.RUnlock()

	return &onDiskDatafile{
		id:           df.id,
		r:            df.r,
		ra:           df.ra,
		w:            nil,
		offset:       df.offset,
		dec:          df.dec,
		enc:          df.enc,
		maxKeySize:   df.maxKeySize,
		maxValueSize: df.maxValueSize,
	}
}
