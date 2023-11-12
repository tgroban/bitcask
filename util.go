package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

var (
	// ErrInvalidKeyFormat ...
	ErrInvalidKeyFormat = errors.New("invalid key format includes +[],")
)

// Raw key:
// +key,type = value
// +name,s = "James"

var (
	delimSep   = []byte{','}
	delimKey   = []byte{'+'} // Key Prefix
	delimStart = []byte{'['} // Start of Key
	delimEnd   = []byte{']'} // End of Key
)

const (
	minByte byte = 0
	maxByte byte = math.MaxUint8
)

type elementType byte

const (
	stringType    elementType = 's'
	hashType      elementType = 'h'
	listType      elementType = 'l'
	sortedSetType elementType = 'z'
	noneType      elementType = '0'
)

func (e elementType) String() string {
	switch byte(e) {
	case 's':
		return "String"
	case 'h':
		return "Hash"
	case 'l':
		return "List"
	case 'z':
		return "SortedSet"
	default:
		return ""
	}
}

func rawKey(key []byte, t elementType) []byte {
	return bytes.Join([][]byte{delimKey, key, delimSep, {byte(t)}}, nil)
}

func verifyKey(key []byte) error {
	err := ErrInvalidKeyFormat
	if bytes.Contains(key, delimSep) {
		return err
	} else if bytes.Contains(key, delimKey) {
		return err
	} else if bytes.Contains(key, delimStart) {
		return err
	} else if bytes.Contains(key, delimEnd) {
		return err
	}
	return nil
}

// Int2Byte returns an 8-byte little endian representation of the integer i
func Int2Byte(i int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

// Byte2Int returns an int from a little endian encoded byte sequence
func Byte2Int(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}
