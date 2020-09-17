package placement

import (
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	netmapV2 "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/stretchr/testify/require"
)

type testBuilder struct {
	vectors []netmap.Nodes
}

func (b testBuilder) BuildPlacement(*object.Address, *netmap.PlacementPolicy) ([]netmap.Nodes, error) {
	return b.vectors, nil
}

func testNode(v uint32) netmapV2.NodeInfo {
	n := netmapV2.NodeInfo{}
	n.SetAddress("/ip4/0.0.0.0/tcp/" + strconv.Itoa(int(v)))

	return n
}

func flattenVectors(vs []netmap.Nodes) netmap.Nodes {
	v := make(netmap.Nodes, 0)

	for i := range vs {
		v = append(v, vs[i]...)
	}

	return v
}

func testPlacement(t *testing.T, sz []int) []netmap.Nodes {
	res := make([]netmap.Nodes, 0, len(sz))
	num := uint32(0)

	for i := range sz {
		ns := make([]netmapV2.NodeInfo, 0, sz[i])

		for j := 0; j < sz[i]; j++ {
			ns = append(ns, testNode(num))
			num++
		}

		res = append(res, netmap.NodesFromV2(ns))
	}

	return res
}

func TestTraverserObjectScenarios(t *testing.T) {
	t.Run("search scenario", func(t *testing.T) {
		nodes := testPlacement(t, []int{2, 3})

		allNodes := flattenVectors(nodes)

		tr, err := NewTraverser(
			UseBuilder(&testBuilder{vectors: nodes}),
			WithoutSuccessTracking(),
		)
		require.NoError(t, err)

		require.True(t, tr.Success())

		for i := range allNodes {
			require.Equal(t, allNodes[i].NetworkAddress(), tr.Next().String())
		}

		require.Nil(t, tr.Next())
		require.True(t, tr.Success())
	})

	t.Run("read scenario", func(t *testing.T) {
		nodes := testPlacement(t, []int{5, 3, 4})

		allNodes := flattenVectors(nodes)

		tr, err := NewTraverser(
			UseBuilder(&testBuilder{vectors: nodes}),
		)
		require.NoError(t, err)

		for i := range allNodes[:len(allNodes)-3] {
			require.Equal(t, allNodes[i].NetworkAddress(), tr.Next().String())
		}

		require.False(t, tr.Success())

		tr.SubmitSuccess()

		require.True(t, tr.Success())

		require.Nil(t, tr.Next())
	})

	t.Run("put scenario", func(t *testing.T) {
		nodes := testPlacement(t, []int{3, 3, 3})
		sucCount := 3

		tr, err := NewTraverser(
			UseBuilder(&testBuilder{vectors: nodes}),
			SuccessAfter(sucCount),
		)
		require.NoError(t, err)

		for i := 0; i < sucCount; i++ {
			require.NotNil(t, tr.Next())
			require.False(t, tr.Success())
			tr.SubmitSuccess()
		}

		require.Nil(t, tr.Next())
		require.True(t, tr.Success())
	})
}
