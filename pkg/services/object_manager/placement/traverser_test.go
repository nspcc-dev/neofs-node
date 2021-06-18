package placement

import (
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/stretchr/testify/require"
)

type testBuilder struct {
	vectors []netmap.Nodes
}

func (b testBuilder) BuildPlacement(*object.Address, *netmap.PlacementPolicy) ([]netmap.Nodes, error) {
	return b.vectors, nil
}

func testNode(v uint32) (n netmap.NodeInfo) {
	n.SetAddress("/ip4/0.0.0.0/tcp/" + strconv.Itoa(int(v)))

	return n
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

func testPlacement(t *testing.T, ss, rs []int) ([]netmap.Nodes, *container.Container) {
	nodes := make([]netmap.Nodes, 0, len(rs))
	replicas := make([]*netmap.Replica, 0, len(rs))
	num := uint32(0)

	for i := range ss {
		ns := make([]netmap.NodeInfo, 0, ss[i])

		for j := 0; j < ss[i]; j++ {
			ns = append(ns, testNode(num))
			num++
		}

		nodes = append(nodes, netmap.NodesFromInfo(ns))

		s := new(netmap.Replica)
		s.SetCount(uint32(rs[i]))

		replicas = append(replicas, s)
	}

	policy := new(netmap.PlacementPolicy)
	policy.SetReplicas(replicas...)

	return nodes, container.New(container.WithPolicy(policy))
}

func assertSameAddress(t *testing.T, ni *netmap.NodeInfo, addr network.Address) {
	var netAddr network.Address

	err := netAddr.FromString(ni.Address())
	require.NoError(t, err)
	require.True(t, netAddr.Equal(addr))
}

func TestTraverserObjectScenarios(t *testing.T) {
	t.Run("search scenario", func(t *testing.T) {
		selectors := []int{2, 3}
		replicas := []int{1, 2}

		nodes, cnr := testPlacement(t, selectors, replicas)

		nodesCopy := copyVectors(nodes)

		tr, err := NewTraverser(
			ForContainer(cnr),
			UseBuilder(&testBuilder{vectors: nodesCopy}),
			WithoutSuccessTracking(),
		)
		require.NoError(t, err)

		for i := range selectors {
			addrs := tr.Next()

			require.Len(t, addrs, len(nodes[i]))

			for j, n := range nodes[i] {
				assertSameAddress(t, n.NodeInfo, addrs[j])
			}
		}

		require.Empty(t, tr.Next())
		require.True(t, tr.Success())
	})

	t.Run("read scenario", func(t *testing.T) {
		selectors := []int{5, 3}
		replicas := []int{2, 2}

		nodes, cnr := testPlacement(t, selectors, replicas)

		nodesCopy := copyVectors(nodes)

		tr, err := NewTraverser(
			ForContainer(cnr),
			UseBuilder(&testBuilder{
				vectors: nodesCopy,
			}),
			SuccessAfter(1),
		)
		require.NoError(t, err)

		for i := 0; i < len(nodes[0]); i++ {
			require.NotNil(t, tr.Next())
		}

		var n network.Address

		err = n.FromString(nodes[1][0].Address())
		require.NoError(t, err)

		require.Equal(t, []network.Address{n}, tr.Next())
	})

	t.Run("put scenario", func(t *testing.T) {
		selectors := []int{5, 3}
		replicas := []int{2, 2}

		nodes, cnr := testPlacement(t, selectors, replicas)

		nodesCopy := copyVectors(nodes)

		tr, err := NewTraverser(
			ForContainer(cnr),
			UseBuilder(&testBuilder{vectors: nodesCopy}),
		)
		require.NoError(t, err)

		fn := func(curVector int) {
			for i := 0; i+replicas[curVector] < selectors[curVector]; i += replicas[curVector] {
				addrs := tr.Next()
				require.Len(t, addrs, replicas[curVector])

				for j := range addrs {
					assertSameAddress(t, nodes[curVector][i+j].NodeInfo, addrs[j])
				}
			}

			require.Empty(t, tr.Next())
			require.False(t, tr.Success())

			for i := 0; i < replicas[curVector]; i++ {
				tr.SubmitSuccess()
			}
		}

		for i := range selectors {
			fn(i)

			if i < len(selectors)-1 {
				require.False(t, tr.Success())
			} else {
				require.True(t, tr.Success())
			}
		}
	})

	t.Run("local operation scenario", func(t *testing.T) {
		selectors := []int{2, 3}
		replicas := []int{1, 2}

		nodes, cnr := testPlacement(t, selectors, replicas)

		tr, err := NewTraverser(
			ForContainer(cnr),
			UseBuilder(&testBuilder{
				vectors: []netmap.Nodes{{nodes[1][1]}}, // single node (local)
			}),
			SuccessAfter(1),
		)
		require.NoError(t, err)

		require.NotEmpty(t, tr.Next())
		require.False(t, tr.Success())

		// add 1 OK
		tr.SubmitSuccess()

		// nothing more to do
		require.Empty(t, tr.Next())

		// common success
		require.True(t, tr.Success())
	})
}
