package attributes_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/util/attributes"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func testAttributeMap(t *testing.T, mSrc, mExp map[string]string) {
	var node netmap.NodeInfo

	s := make([]string, 0, len(mSrc))
	for k, v := range mSrc {
		s = append(s, k+":"+v)
	}

	err := attributes.ReadNodeAttributes(&node, s)
	require.NoError(t, err)

	if mExp == nil {
		mExp = mSrc
	}

	for key, value := range node.Attributes() {
		v, ok := mExp[key]
		require.True(t, ok)
		require.Equal(t, value, v)
		delete(mExp, key)
	}

	require.Empty(t, mExp)
}

func TestParseV2Attributes(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var node netmap.NodeInfo
		err := attributes.ReadNodeAttributes(&node, nil)
		require.NoError(t, err)
		require.Zero(t, node.NumberOfAttributes())
	})

	t.Run("empty key and/or value", func(t *testing.T) {
		var node netmap.NodeInfo
		err := attributes.ReadNodeAttributes(&node, []string{
			":HDD",
		})
		require.Error(t, err)

		err = attributes.ReadNodeAttributes(&node, []string{
			"StorageType:",
		})
		require.Error(t, err)

		err = attributes.ReadNodeAttributes(&node, []string{
			":",
		})
		require.Error(t, err)
	})

	t.Run("non-unique keys", func(t *testing.T) {
		var node netmap.NodeInfo
		err := attributes.ReadNodeAttributes(&node, []string{
			"StorageType:HDD",
			"StorageType:HDD",
		})
		require.Error(t, err)
	})

	t.Run("malformed", func(t *testing.T) {
		var node netmap.NodeInfo
		err := attributes.ReadNodeAttributes(&node, []string{"..."})
		require.Error(t, err)

		err = attributes.ReadNodeAttributes(&node, []string{"a:b", ""})
		require.Error(t, err)

		err = attributes.ReadNodeAttributes(&node, []string{"//"})
		require.Error(t, err)
	})

	t.Run("correct", func(t *testing.T) {
		testAttributeMap(t, map[string]string{
			"Location":    "Europe",
			"StorageType": "HDD",
		}, nil)
	})

	t.Run("escape characters", func(t *testing.T) {
		testAttributeMap(t, map[string]string{
			`K\:ey1`: `V\/alue`,
			`Ke\/y2`: `Va\:lue`,
		}, map[string]string{
			`K:ey1`:  `V\/alue`,
			`Ke\/y2`: `Va:lue`,
		})
	})
}
