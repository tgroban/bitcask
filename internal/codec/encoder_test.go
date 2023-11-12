package codec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mills.io/bitcask/v2/internal"
)

func BenchmarkEncoder(b *testing.B) {
	var buf bytes.Buffer
	encoder := NewEncoder(&buf)

	entry := internal.Entry{
		Key:      []byte("mykey"),
		Value:    []byte("myvalue"),
		Checksum: 414141,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := encoder.Encode(entry)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestEncode(t *testing.T) {
	var buf bytes.Buffer
	encoder := NewEncoder(&buf)

	entry := internal.Entry{
		Key:      []byte("mykey"),
		Value:    []byte("myvalue"),
		Checksum: 414141,
	}
	expected := []byte{0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7, 0x6d, 0x79, 0x6b, 0x65, 0x79, 0x6d, 0x79, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x0, 0x6, 0x51, 0xbd}

	_, err := encoder.Encode(entry)
	require.NoError(t, err)

	actual := buf.Bytes()
	assert.EqualValues(t, expected, actual)
}
