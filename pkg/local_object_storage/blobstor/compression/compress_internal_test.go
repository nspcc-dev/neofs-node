package compression

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrefixLen(t *testing.T) {
	require.Len(t, zstdFrameMagic, PrefixLength)
}
