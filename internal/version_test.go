package internal

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFullVersion(t *testing.T) {
	expected := fmt.Sprintf("%s@%s", Version, Commit)
	assert.Equal(t, expected, FullVersion())
}
