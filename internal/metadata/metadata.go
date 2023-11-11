package metadata

import (
	"os"

	"go.mills.io/bitcask/internal"
)

type MetaData struct {
	IndexUpToDate    bool  `json:"index_up_to_date"`
	ReclaimableSpace int64 `json:"reclaimable_space"`
}

func (m *MetaData) Save(path string, mode os.FileMode) error {
	return internal.SaveJSONToFile(m, path, mode)
}

func Load(path string) (*MetaData, error) {
	var m MetaData
	err := internal.LoadFromJSONFile(path, &m)
	return &m, err
}
