package codec

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mills.io/bitcask/v2/internal"
)

func BenchmarkDecoder(b *testing.B) {
	data := []byte{0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7, 0x6d, 0x79, 0x6b, 0x65, 0x79, 0x6d, 0x79, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x0, 0x6, 0x51, 0xbd}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		decoder := NewDecoder(bytes.NewBuffer(data), 16, 32)
		b.StartTimer()

		_, err := decoder.Decode(&internal.Entry{})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestDecoder(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7, 0x6d, 0x79, 0x6b, 0x65, 0x79, 0x6d, 0x79, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x0, 0x6, 0x51, 0xbd})
	decoder := NewDecoder(buf, 16, 32)

	expected := internal.Entry{
		Key:      []byte("mykey"),
		Value:    []byte("myvalue"),
		Checksum: 414141,
	}
	actual := internal.Entry{}
	_, err := decoder.Decode(&actual)
	require.NoError(t, err)
	assert.EqualValues(t, expected, actual)
}

func TestDecodeOnNilEntry(t *testing.T) {
	var buf bytes.Buffer
	decoder := NewDecoder(&buf, 1, 1)

	_, err := decoder.Decode(nil)
	if assert.Error(t, err) {
		assert.Equal(t, errCantDecodeOnNilEntry, err)
	}
}

func TestShortPrefix(t *testing.T) {
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
	actual := internal.Entry{}
	buf := []byte{0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7, 0x6d, 0x79, 0x6b, 0x65, 0x79, 0x6d, 0x79, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x0, 0x6, 0x51, 0xbd}
	valueOffset := uint32(5)
	expected := internal.Entry{
		Key:      []byte("mykey"),
		Value:    []byte("myvalue"),
		Checksum: 414141,
	}
	decodeWithoutPrefix(buf[keySize+valueSize:], valueOffset, &actual)
	assert.Equal(t, expected.Key, actual.Key)
	assert.Equal(t, expected.Value, actual.Value)
	assert.Equal(t, expected.Checksum, actual.Checksum)
}
