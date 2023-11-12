package bitcask

import (
	"bytes"
	"errors"
)

func (b *bitcask) List(key Key) *List {
	return &List{db: b, key: key}
}

// List ...
// +key,l = ""
// l[key]0 = "a"
// l[key]1 = "b"
// l[key]2 = "c"
type List struct {
	db  DB
	key Key
}

// Index ...
func (l *List) Index(i int64) ([]byte, error) {
	x, err := l.leftIndex()
	if err != nil {
		return nil, err
	}
	return l.db.Get(l.indexKey(x + i))
}

// Range enumerate value by index
// <start> must >= 0
// <stop> should equal to -1 or lager than <start>
func (l *List) Range(start, stop int64, fn func(i int64, value []byte, quit *bool)) error {
	if start < 0 || (stop != -1 && start > stop) {
		return errors.New("bad start/stop index")
	}
	x, y, err := l.rangeIndex()
	if err != nil {
		return err
	}
	if stop == -1 {
		stop = (y - x + 1) - 1 // (size) - 1
	}
	min := l.indexKey(x + int64(start))
	max := l.indexKey(x + int64(stop))
	var i int64 // 0
	ErrStopIteration := errors.New("err: stop iteration")
	err = l.db.Scan(min, func(key Key) error {
		if key != nil && bytes.Compare(key, max) <= 0 {
			val, err := l.db.Get(key)
			if err != nil {
				return err
			}
			quit := false
			if fn(start+i, val, &quit); quit {
				return ErrStopIteration
			}
			i++
			return nil
		}
		return ErrStopIteration
	})
	if err == ErrStopIteration {
		return nil
	}
	return err
}

// Append ...
func (l *List) Append(values ...Value) error {
	x, y, err := l.rangeIndex()
	if err != nil {
		return err
	}
	if x == 0 && y == -1 {
		if err := l.db.Put(l.rawKey(), nil); err != nil {
			return err
		}
	}
	for i, val := range values {
		if err := l.db.Put(l.indexKey(y+int64(i)+1), val); err != nil {
			return err
		}
	}
	return nil
}

// Pop ...
func (l *List) Pop() ([]byte, error) {
	x, y, err := l.rangeIndex()
	if err != nil {
		return nil, err
	}

	size := y - x + 1
	if size == 0 {
		return nil, nil
	} else if size < 0 { // double check
		return nil, errors.New("bad list struct")
	}

	keyIndex := l.indexKey(y)

	val, err := l.db.Get(keyIndex)
	if err != nil {
		return nil, err
	}
	if err := l.db.Delete(keyIndex); err != nil {
		return nil, err
	}
	if size == 1 { // clean up
		return nil, l.db.Delete(l.rawKey())
	}

	return val, nil
}

// Len ...
func (l *List) Len() (int64, error) {
	x, y, err := l.rangeIndex()
	return y - x + 1, err
}

func (l *List) rangeIndex() (int64, int64, error) {
	left, err := l.leftIndex()
	if err != nil {
		return 0, -1, err
	}
	right, err := l.rightIndex()
	if err != nil {
		return 0, -1, err
	}
	return left, right, nil
}

func (l *List) leftIndex() (int64, error) {
	idx := int64(0) // default 0
	prefix := l.keyPrefix()
	ErrStopIteration := errors.New("err: stop iteration")
	err := l.db.Scan(prefix, func(key Key) error {
		if bytes.HasPrefix(key, prefix) {
			idx = l.indexInKey(key)
		}
		return ErrStopIteration
	})
	if err == ErrStopIteration {
		return idx, nil
	}
	return idx, err
}

func (l *List) rightIndex() (int64, error) {
	idx := int64(-1) // default -1
	prefix := l.keyPrefix()
	err := l.db.Scan(prefix, func(key Key) error {
		if bytes.HasPrefix(key, prefix) {
			idx = l.indexInKey(key)
		}
		return nil
	})
	return idx, err
}

// +key,l = ""
func (l *List) rawKey() []byte {
	return rawKey(l.key, elementType(listType))
}

// l[key]
func (l *List) keyPrefix() []byte {
	return bytes.Join([][]byte{{byte(listType)}, delimStart, l.key, delimEnd}, nil)
}

// l[key]0 = "a"
func (l *List) indexKey(i int64) []byte {
	sign := []byte{0}
	if i >= 0 {
		sign = []byte{1}
	}
	b := bytes.Join([][]byte{l.keyPrefix(), sign, Int2Byte(i)}, nil)
	return b
}

// split l[key]index into index
func (l *List) indexInKey(key []byte) int64 {
	idxbuf := bytes.TrimPrefix(key, l.keyPrefix())
	return Byte2Int(idxbuf[1:]) // skip sign "0/1"
}
