package codec

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mills.io/bitcask/v2/internal"
)

func TestDecodeOnNilEntry(t *testing.T) {
	t.Parallel()
	decoder := NewDecoder(&bytes.Buffer{}, 1, 1)

	_, err := decoder.Decode(nil)
	if assert.Error(t, err) {
		assert.Equal(t, errCantDecodeOnNilEntry, err)
	}
}

func TestShortPrefix(t *testing.T) {
	t.Parallel()
	maxKeySize, maxValueSize := uint32(10), uint64(20)
	prefix := make([]byte, keySize+valueSize)
	binary.BigEndian.PutUint32(prefix, 1)
	binary.BigEndian.PutUint64(prefix[keySize:], 1)

	truncBytesCount := 2
	buf := bytes.NewBuffer(prefix[:keySize+valueSize-truncBytesCount])
	decoder := NewDecoder(buf, maxKeySize, maxValueSize)
	_, err := decoder.Decode(&internal.Entry{})
	if assert.Error(t, err) {
		assert.Equal(t, io.ErrUnexpectedEOF, err)
	}
}

func TestInvalidValueKeySizes(t *testing.T) {
	maxKeySize, maxValueSize := uint32(10), uint64(20)

	tests := []struct {
		keySize   uint32
		valueSize uint64
		name      string
	}{
		{keySize: 0, valueSize: 5, name: "zero key size"}, //zero value size is correct for tombstones
		{keySize: 11, valueSize: 5, name: "key size overflow"},
		{keySize: 5, valueSize: 21, name: "value size overflow"},
		{keySize: 11, valueSize: 21, name: "key and value size overflow"},
	}

	for i := range tests {
		i := i
		t.Run(tests[i].name, func(t *testing.T) {
			t.Parallel()
			prefix := make([]byte, keySize+valueSize)
			binary.BigEndian.PutUint32(prefix, tests[i].keySize)
			binary.BigEndian.PutUint64(prefix[keySize:], tests[i].valueSize)

			buf := bytes.NewBuffer(prefix)
			decoder := NewDecoder(buf, maxKeySize, maxValueSize)
			_, err := decoder.Decode(&internal.Entry{})
			if assert.Error(t, err) {
				assert.Equal(t, errInvalidKeyOrValueSize, err)
			}
		})
	}
}

func TestTruncatedData(t *testing.T) {
	maxKeySize, maxValueSize := uint32(10), uint64(20)

	key := []byte("foo")
	value := []byte("bar")
	data := make([]byte, keySize+valueSize+len(key)+len(value)+checksumSize)

	binary.BigEndian.PutUint32(data, uint32(len(key)))
	binary.BigEndian.PutUint64(data[keySize:], uint64(len(value)))
	copy(data[keySize+valueSize:], key)
	copy(data[keySize+valueSize+len(key):], value)
	copy(data[keySize+valueSize+len(key)+len(value):], bytes.Repeat([]byte("0"), checksumSize))

	tests := []struct {
		data []byte
		name string
	}{
		{data: data[:keySize+valueSize+len(key)-1], name: "truncated key"},
		{data: data[:keySize+valueSize+len(key)+len(value)-1], name: "truncated value"},
		{data: data[:keySize+valueSize+len(key)+len(value)+checksumSize-1], name: "truncated checksum"},
	}

	for i := range tests {
		i := i
		t.Run(tests[i].name, func(t *testing.T) {
			t.Parallel()
			buf := bytes.NewBuffer(tests[i].data)
			decoder := NewDecoder(buf, maxKeySize, maxValueSize)
			_, err := decoder.Decode(&internal.Entry{})
			if assert.Error(t, err) {
				assert.Equal(t, errTruncatedData, err)
			}
		})
	}
}

func TestDecodeWithoutPrefix(t *testing.T) {
	e := internal.Entry{}
	buf := []byte{0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 7, 109, 121, 107, 101, 121, 109, 121, 118, 97, 108, 117, 101, 0, 6, 81, 189, 0, 0, 0, 0, 95, 117, 28, 0}
	valueOffset := uint32(5)
	expectedEntry := internal.Entry{
		Key:      []byte("mykey"),
		Value:    []byte("myvalue"),
		Checksum: 414141,
	}
	decodeWithoutPrefix(buf[keySize+valueSize:], valueOffset, &e)
	assert.Equal(t, expectedEntry.Key, e.Key)
	assert.Equal(t, expectedEntry.Value, e.Value)
	assert.Equal(t, expectedEntry.Checksum, e.Checksum)
}
