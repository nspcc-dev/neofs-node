package structure_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/structure"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func TestValidator_Verify(t *testing.T) {
	v := structure.New()
	t.Run("duplicated attributes", func(t *testing.T) {
		var n netmap.NodeInfo
		n.SetAttributes([][2]string{
			{"key1", "val1"},
			{"key2", "val2"},
			{"key1", "val3"},
			{"key2", "val4"},
		})
		require.EqualError(t, v.Verify(n), "repeating node attribute: 'key1'")
	})
}
