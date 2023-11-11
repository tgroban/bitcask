package index

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"testing"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
	"github.com/pkg/errors"
	"go.mills.io/bitcask/internal"
)

const (
	base64SampleTree = "AAAABGFiY2QAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARhYmNlAAAAAQAAAAAAAAABAAAAAAAAAAEAAAAEYWJjZgAAAAIAAAAAAAAAAgAAAAAAAAACAAAABGFiZ2QAAAADAAAAAAAAAAMAAAAAAAAAAw=="
)

func TestWriteIndex(t *testing.T) {
	at, expectedSerializedSize := getSampleTree()

	var b bytes.Buffer
	err := writeIndex(at, &b)
	if err != nil {
		t.Fatalf("writing index failed: %v", err)
	}
	if b.Len() != expectedSerializedSize {
		t.Fatalf("incorrect size of serialised index: expected %d, got: %d", expectedSerializedSize, b.Len())
	}
	sampleTreeBytes, _ := base64.StdEncoding.DecodeString(base64SampleTree)
	if !bytes.Equal(b.Bytes(), sampleTreeBytes) {
		t.Fatalf("unexpected serialization of the tree")
	}
}

func TestReadIndex(t *testing.T) {
	sampleTreeBytes, _ := base64.StdEncoding.DecodeString(base64SampleTree)
	b := bytes.NewBuffer(sampleTreeBytes)

	var err error
	at := iradix.New[internal.Item]()
	at, err = readIndex(b, at, 1024)
	if err != nil {
		t.Fatalf("error while deserializing correct sample tree: %v", err)
	}

	atsample, _ := getSampleTree()
	if atsample.Len() != at.Len() {
		t.Fatalf("trees aren't the same size, expected %v, got %v", atsample.Len(), at.Len())
	}
	atsample.Root().Walk(func(key []byte, item internal.Item) bool {
		_, found := at.Root().Get(key)
		if !found {
			t.Fatalf("expected node wasn't found: %s", key)
		}
		return false
	})
}

func TestReadCorruptedData(t *testing.T) {
	sampleBytes, _ := base64.StdEncoding.DecodeString(base64SampleTree)

	t.Run("truncated", func(t *testing.T) {
		table := []struct {
			name string
			err  error
			data []byte
		}{
			{name: "key-size-first-item", err: errTruncatedKeySize, data: sampleBytes[:2]},
			{name: "key-data-second-item", err: errTruncatedKeyData, data: sampleBytes[:6]},
			{name: "key-size-second-item", err: errTruncatedKeySize, data: sampleBytes[:(int32Size+4+fileIDSize+offsetSize+sizeSize)+2]},
			{name: "key-data-second-item", err: errTruncatedKeyData, data: sampleBytes[:(int32Size+4+fileIDSize+offsetSize+sizeSize)+6]},
			{name: "data", err: errTruncatedData, data: sampleBytes[:int32Size+4+(fileIDSize+offsetSize+sizeSize-3)]},
		}

		for i := range table {
			t.Run(table[i].name, func(t *testing.T) {
				bf := bytes.NewBuffer(table[i].data)

				if _, err := readIndex(bf, iradix.New[internal.Item](), 1024); !IsIndexCorruption(err) || errors.Cause(err) != table[i].err {
					t.Fatalf("expected %v, got %v", table[i].err, err)
				}
			})
		}
	})

	t.Run("overflow", func(t *testing.T) {
		overflowKeySize := make([]byte, len(sampleBytes))
		copy(overflowKeySize, sampleBytes)
		binary.BigEndian.PutUint32(overflowKeySize, 1025)

		overflowDataSize := make([]byte, len(sampleBytes))
		copy(overflowDataSize, sampleBytes)
		binary.BigEndian.PutUint32(overflowDataSize[int32Size+4+fileIDSize+offsetSize:], 1025)

		table := []struct {
			name       string
			err        error
			maxKeySize uint32
			data       []byte
		}{
			{name: "key-data-overflow", err: errKeySizeTooLarge, maxKeySize: 1024, data: overflowKeySize},
		}

		for i := range table {
			t.Run(table[i].name, func(t *testing.T) {
				bf := bytes.NewBuffer(table[i].data)

				if _, err := readIndex(bf, iradix.New[internal.Item](), table[i].maxKeySize); !IsIndexCorruption(err) || errors.Cause(err) != table[i].err {
					t.Fatalf("expected %v, got %v", table[i].err, err)
				}
			})
		}
	})

}

func getSampleTree() (*iradix.Tree[internal.Item], int) {
	at := iradix.New[internal.Item]()
	keys := [][]byte{[]byte("abcd"), []byte("abce"), []byte("abcf"), []byte("abgd")}
	expectedSerializedSize := 0
	for i := range keys {
		at, _, _ = at.Insert(keys[i], internal.Item{FileID: i, Offset: int64(i), Size: int64(i)})
		expectedSerializedSize += int32Size + len(keys[i]) + fileIDSize + offsetSize + sizeSize
	}

	return at, expectedSerializedSize
}
