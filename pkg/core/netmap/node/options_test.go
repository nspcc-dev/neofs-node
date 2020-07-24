package node

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/stretchr/testify/require"
)

func TestInfo_Price(t *testing.T) {
	var info Info

	// too small value
	info.opts = []string{"/Price:0.01048575"}
	require.Equal(t, uint64(0), info.Price())

	// min value
	info.opts = []string{"/Price:0.01048576"}
	require.Equal(t, uint64(1), info.Price())

	// big value
	info.opts = []string{"/Price:1000000000.666"}
	require.Equal(t, uint64(1000000000.666*1e8/object.UnitsMB), info.Price())
}
