package data

import (
	"fmt"
	"sync"

	"github.com/mattetti/filebuffer"

	"go.mills.io/bitcask/v2/internal"
	"go.mills.io/bitcask/v2/internal/codec"
)

type inMemoryDatafile struct {
	sync.RWMutex

	id           int
	buf          *filebuffer.Buffer
	offset       int64
	dec          *codec.Decoder
	enc          *codec.Encoder
	maxKeySize   uint32
	maxValueSize uint64
}

func (df *inMemoryDatafile) FileID() int {
	return df.id
}

func (df *inMemoryDatafile) Name() string {
	return fmt.Sprintf("in-memory-%d", df.id)
}

func (df *inMemoryDatafile) Close() error {
	return nil
}

func (df *inMemoryDatafile) Sync() error {
	return nil
}

func (df *inMemoryDatafile) Size() int64 {
	df.RLock()
	defer df.RUnlock()
	return df.offset
}

// Read reads the next entry from the datafile
func (df *inMemoryDatafile) Read() (e internal.Entry, n int64, err error) {
	df.Lock()
	defer df.Unlock()

	n, err = df.dec.Decode(&e)
	if err != nil {
		return
	}

	return
}

// ReadAt the entry located at index offset with expected serialized size
func (df *inMemoryDatafile) ReadAt(index, size int64) (e internal.Entry, err error) {
	b := make([]byte, size)

	df.RLock()
	defer df.RUnlock()

	n, err := df.buf.ReadAt(b, index)
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

func (df *inMemoryDatafile) Write(e internal.Entry) (int64, int64, error) {
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

func (df *inMemoryDatafile) Readonly() bool { return true }

func (df *inMemoryDatafile) ReopenReadonly() Datafile {
	// No reason for this type of datafile to ever by readonly.
	return df
}
