package object

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLimits(t *testing.T) {
	varintB := make([]byte, binary.MaxVarintLen64)
	require.EqualValues(t, binary.PutUvarint(varintB, maxGetResponseChunkLen), maxGetResponseChunkVarintLen)
}
