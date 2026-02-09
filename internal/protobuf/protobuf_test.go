package protobuf_test

import (
	"testing"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/stretchr/testify/require"
)

func TestFieldBounds_IsMissing(t *testing.T) {
	var f iprotobuf.FieldBounds
	require.True(t, f.IsMissing())

	f.To = 10
	require.False(t, f.IsMissing())

	f.From = 9
	require.False(t, f.IsMissing())

	f.To = 9
	require.True(t, f.IsMissing())
}
