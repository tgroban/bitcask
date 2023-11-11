package codec

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mills.io/bitcask/internal"
)

func TestEncode(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	encoder := NewEncoder(&buf)
	_, err := encoder.Encode(internal.Entry{
		Key:      []byte("mykey"),
		Value:    []byte("myvalue"),
		Checksum: 414141,
	})

	expectedHex := "0000000500000000000000076d796b65796d7976616c7565000651bd000000005f751c00"
	if assert.NoError(t, err) {
		assert.Equal(t, expectedHex, hex.EncodeToString(buf.Bytes()))
	}
}
