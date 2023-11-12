package bitcask

import (
	"bytes"
	"errors"
)

func (b *bitcask) Hash(key Key) *Hash {
	return &Hash{db: b, key: key}
}

// Hash ...
//
//	+key,h = ""
//	h[key]name = "James"
//	h[key]age = "21"
//	h[key]sex = "Male"
type Hash struct {
	db  DB
	key Key
}

// Get ...
func (h *Hash) Get(field []byte) ([]byte, error) {
	return h.db.Get(h.fieldKey(field))
}

// MGet ...
func (h *Hash) MGet(fields ...[]byte) ([][]byte, error) {
	values := make([][]byte, 0, len(fields))
	for _, field := range fields {
		val, err := h.db.Get(h.fieldKey(field))
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// GetAll ...
func (h *Hash) GetAll() (map[string][]byte, error) {
	pairs := map[string][]byte{}
	prefix := h.fieldPrefix()
	err := h.db.Scan(prefix, func(key Key) error {
		val, err := h.db.Get(key)
		if err != nil {
			return err
		}
		pairs[string(h.fieldInKey(key))] = val
		return nil
	})
	return pairs, err
}

// Set ...
func (h *Hash) Set(field, value []byte) error {
	return h.MSet(field, value)
}

// MSet ...
func (h *Hash) MSet(pairs ...[]byte) error {
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		return errors.New("invalid field value pairs")
	}

	for i := 0; i < len(pairs); i += 2 {
		field, val := pairs[i], pairs[i+1]
		if err := h.db.Put(h.fieldKey(field), val); err != nil {
			return err
		}
	}
	return h.db.Put(h.rawKey(), nil)
}

// Remove ...
func (h *Hash) Remove(fields ...[]byte) error {
	for _, field := range fields {
		if err := h.db.Delete(h.fieldKey(field)); err != nil {
			return err
		}
	}
	// clean up
	prefix := h.fieldPrefix()
	return h.db.Scan(prefix, func(key Key) error {
		return h.db.Delete(key)
	})
}

// Drop ...
func (h *Hash) Drop() error {
	prefix := h.fieldPrefix()
	err := h.db.Scan(prefix, func(key Key) error {
		return h.db.Delete(key)
	})
	if err != nil {
		return err
	}
	return h.db.Delete(h.rawKey())
}

// +key,h
func (h *Hash) rawKey() []byte {
	return rawKey(h.key, hashType)
}

// h[key]field
func (h *Hash) fieldKey(field []byte) []byte {
	return bytes.Join([][]byte{h.fieldPrefix(), field}, nil)
}

// h[key]
func (h *Hash) fieldPrefix() []byte {
	return bytes.Join([][]byte{{byte(hashType)}, delimStart, h.key, delimEnd}, nil)
}

// split h[key]field into field
func (h *Hash) fieldInKey(fieldKey []byte) []byte {
	right := bytes.Index(fieldKey, delimEnd)
	return fieldKey[right+1:]
}
