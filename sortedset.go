package bitcask

import (
	"bytes"
	"errors"
	"log"
)

func (b *bitcask) SortedSet(key Key) *SortedSet {
	return &SortedSet{db: b, key: key}
}

// SortedSet ...
// +key,z = ""
// z[key]m member = score
// z[key]s score member = ""
type SortedSet struct {
	db  DB
	key Key
}

// Add add score & member pairs
// SortedSet.Add(Score, []byte, Score, []byte ...)
func (s *SortedSet) Add(scoreMembers ...[]byte) (int, error) {
	count := len(scoreMembers)
	if count < 2 || count%2 != 0 {
		return 0, errors.New("invalid score/member pairs")
	}
	added := 0
	for i := 0; i < count; i += 2 {
		score, member := scoreMembers[i], scoreMembers[i+1]
		scoreKey, memberKey := s.scoreKey(score, member), s.memberKey(member)
		oldScore, err := s.db.Get(memberKey)
		if err != nil && err != ErrKeyNotFound {
			return added, err
		}
		// remove old score key
		if oldScore != nil {
			oldScoreKey := s.scoreKey(oldScore, member)
			if err := s.db.Delete(oldScoreKey); err != nil {
				return added, err
			}
		} else {
			added++
		}
		if err := s.db.Put(memberKey, score); err != nil {
			return added, err
		}
		if err := s.db.Put(scoreKey, Value("\xff")); err != nil {
			return added, err
		}
	}
	if err := s.db.Put(s.rawKey(), nil); err != nil {
		return added, err
	}
	return added, nil
}

// Score ...
func (s *SortedSet) Score(member []byte) (Score, error) {
	value, err := s.db.Get(s.memberKey(member))
	if err != nil {
		return nil, err
	}
	return Score(value), nil
}

// Remove ...
func (s *SortedSet) Remove(members ...[]byte) (int, error) {
	removed := 0 // not including non existing members
	for _, member := range members {
		score, err := s.db.Get(s.memberKey(member))
		if err != nil {
			return removed, err
		}
		if score == nil {
			continue
		}
		if err := s.db.Delete(s.scoreKey(score, member)); err != nil {
			return removed, err
		}
		if err := s.db.Delete(s.memberKey(member)); err != nil {
			return removed, err
		}
		removed++
	}
	// clean up
	prefix := s.keyPrefix()
	ErrStopIteration := errors.New("err: stop iteration")
	err := s.db.Scan(prefix, func(key Key) error {
		if !bytes.HasPrefix(key, prefix) {
			if err := s.db.Delete(s.rawKey()); err != nil {
				return err
			}
		}
		return ErrStopIteration
	})
	if err != ErrStopIteration {
		return removed, err
	}
	return removed, nil
}

// Range ...
// <from> is less than <to>
func (s *SortedSet) Range(from, to Score, fn func(i int64, score Score, member []byte, quit *bool)) error {
	min := s.scorePrefix(from)
	max := append(s.scorePrefix(to), maxByte)
	var i int64 // 0
	ErrStopIteration := errors.New("err: stop iteration")
	err := s.db.Range(min, max, func(key Key) error {
		quit := false
		score, member, err := s.splitScoreKey(key)
		if err != nil {
			log.Printf("err: %v\n", err)
			return err
		}
		if fn(i, score, member, &quit); quit {
			log.Println("stop iteration")
			return ErrStopIteration
		}
		i++
		return nil
	})
	if err != ErrStopIteration {
		return err
	}
	return nil
}

// +key,z = ""
func (s *SortedSet) rawKey() []byte {
	return rawKey(s.key, elementType(sortedSetType))
}

// z[key]
func (s *SortedSet) keyPrefix() []byte {
	return bytes.Join([][]byte{{byte(sortedSetType)}, delimStart, s.key, delimEnd}, nil)
}

// z[key]m
func (s *SortedSet) memberKey(member []byte) []byte {
	return bytes.Join([][]byte{s.keyPrefix(), {'m'}, member}, nil)
}

// z[key]s score
func (s *SortedSet) scorePrefix(score []byte) []byte {
	if score == nil {
		return bytes.Join([][]byte{s.keyPrefix(), {'s'}}, nil)
	}
	return bytes.Join([][]byte{s.keyPrefix(), {'s'}, score, {' '}}, nil)
}

// z[key]s score member
func (s *SortedSet) scoreKey(score, member []byte) []byte {
	return bytes.Join([][]byte{s.keyPrefix(), {'s'}, score, {' '}, member}, nil)
}

// split (z[key]s score member) into (score, member)
func (s *SortedSet) splitScoreKey(scoreKey []byte) ([]byte, []byte, error) {
	buf := bytes.TrimPrefix(scoreKey, s.keyPrefix())
	pairs := bytes.Split(buf[1:], []byte{' '}) // skip score mark 's'
	if len(pairs) != 2 {
		return nil, nil, errors.New("invalid score/member key: " + string(scoreKey))
	}
	return pairs[0], pairs[1], nil
}

// split (z[key]m member) into (member)
func (s *SortedSet) splitMemberKey(memberKey []byte) ([]byte, error) {
	buf := bytes.TrimPrefix(memberKey, s.keyPrefix())
	return buf[1:], nil // skip member mark 'm'
}
