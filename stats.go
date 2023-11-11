package bitcask

import "go.mills.io/bitcask/internal"

// Stats is a struct returned by Stats() on an open bitcask instance
type Stats struct {
	Datafiles   int
	Keys        int
	Size        int64
	Reclaimable int64
}

// Stats returns statistics about the database including the number of
// data files, keys and overall size on disk of the data
func (b *bitcask) Stats() (stats Stats, err error) {
	if stats.Size, err = internal.DirSize(b.path); err != nil {
		return
	}

	stats.Datafiles = len(b.datafiles)
	stats.Keys = b.trie.Len()
	stats.Reclaimable = b.metadata.ReclaimableSpace

	return
}
