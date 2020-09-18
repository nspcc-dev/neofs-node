package placement

import (
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
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

func copyVectors(v []netmap.Nodes) []netmap.Nodes {
	vc := make([]netmap.Nodes, 0, len(v))

	for i := range v {
		ns := make(netmap.Nodes, len(v[i]))
		copy(ns, v[i])

		vc = append(vc, ns)
	}

	return vc
}

func testPlacement(t *testing.T, rs, ss []int) ([]netmap.Nodes, *container.Container) {
	nodes := make([]netmap.Nodes, 0, len(rs))
	selectors := make([]*netmap.Selector, 0, len(rs))
	num := uint32(0)

	for i := range rs {
		ns := make([]netmapV2.NodeInfo, 0, rs[i])

		for j := 0; j < rs[i]; j++ {
			ns = append(ns, testNode(num))
			num++
		}

		nodes = append(nodes, netmap.NodesFromV2(ns))

		s := new(netmap.Selector)
		s.SetCount(uint32(ss[i]))

		selectors = append(selectors, s)
	}

	policy := new(netmap.PlacementPolicy)
	policy.SetSelectors(selectors)

	return nodes, container.New(container.WithPolicy(policy))
}

func TestTraverserObjectScenarios(t *testing.T) {
	t.Run("search scenario", func(t *testing.T) {
		replicas := []int{2, 3}
		selectors := []int{1, 2}

		nodes, cnr := testPlacement(t, replicas, selectors)

		nodesCopy := copyVectors(nodes)

		tr, err := NewTraverser(
			ForContainer(cnr),
			UseBuilder(&testBuilder{vectors: nodesCopy}),
			WithoutSuccessTracking(),
		)
		require.NoError(t, err)

		for i := range replicas {
			addrs := tr.Next()

			require.Len(t, addrs, len(nodes[i]))

			for j, n := range nodes[i] {
				require.Equal(t, n.NetworkAddress(), addrs[j].String())
			}
		}

		require.Empty(t, tr.Next())
		require.True(t, tr.Success())
	})

	t.Run("read scenario", func(t *testing.T) {
		replicas := []int{5, 3}
		selectors := []int{2, 2}

		nodes, cnr := testPlacement(t, replicas, selectors)

		nodesCopy := copyVectors(nodes)

		tr, err := NewTraverser(
			ForContainer(cnr),
			UseBuilder(&testBuilder{vectors: nodesCopy}),
			SuccessAfter(1),
		)
		require.NoError(t, err)

		fn := func(curVector int) {
			for i := 0; i < replicas[curVector]; i++ {
				addrs := tr.Next()
				require.Len(t, addrs, 1)

				require.Equal(t, nodes[curVector][i].NetworkAddress(), addrs[0].String())
			}

			require.Empty(t, tr.Next())
			require.False(t, tr.Success())

			tr.SubmitSuccess()
		}

		for i := range replicas {
			fn(i)

			if i < len(replicas)-1 {
				require.False(t, tr.Success())
			} else {
				require.True(t, tr.Success())
			}
		}
	})

	t.Run("put scenario", func(t *testing.T) {
		replicas := []int{5, 3}
		selectors := []int{2, 2}

		nodes, cnr := testPlacement(t, replicas, selectors)

		nodesCopy := copyVectors(nodes)

		tr, err := NewTraverser(
			ForContainer(cnr),
			UseBuilder(&testBuilder{vectors: nodesCopy}),
		)
		require.NoError(t, err)

		fn := func(curVector int) {
			for i := 0; i+selectors[curVector] < replicas[curVector]; i += selectors[curVector] {
				addrs := tr.Next()
				require.Len(t, addrs, selectors[curVector])

				for j := range addrs {
					require.Equal(t, nodes[curVector][i+j].NetworkAddress(), addrs[j].String())
				}
			}

			require.Empty(t, tr.Next())
			require.False(t, tr.Success())

			for i := 0; i < selectors[curVector]; i++ {
				tr.SubmitSuccess()
			}
		}

		for i := range replicas {
			fn(i)

			if i < len(replicas)-1 {
				require.False(t, tr.Success())
			} else {
				require.True(t, tr.Success())
			}
		}
	})
}
