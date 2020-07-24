package netmap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNetMap_Nodes(t *testing.T) {
	nm := New()

	info1 := Info{}
	info1.SetPublicKey([]byte{1, 2, 3})

	info2 := Info{}
	info2.SetPublicKey([]byte{4, 5, 6})

	nodes := []Info{
		info1,
		info2,
	}

	nm.SetNodes(nodes)

	require.Equal(t, nodes, nm.Nodes())
}

func TestNetMap_Root(t *testing.T) {
	nm := New()

	bucket := &Bucket{
		Key:   "key",
		Value: "value",
	}

	nm.SetRoot(bucket)

	require.Equal(t, bucket, nm.Root())
}
