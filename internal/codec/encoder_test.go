package codec

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mills.io/bitcask/v2/internal"
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
	fmt.Printf("buf: %#v\n", buf.Bytes())

	expectedHex := "0000000500000000000000076d796b65796d7976616c7565000651bd"
	if assert.NoError(t, err) {
		assert.Equal(t, expectedHex, hex.EncodeToString(buf.Bytes()))
	}
}
