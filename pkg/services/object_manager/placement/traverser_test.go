package placement

import (
	"strconv"
	"testing"

	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

type testBuilder struct {
	vectors [][]netmap.NodeInfo
}

func (b testBuilder) BuildPlacement(cid.ID, *oid.ID, netmap.PlacementPolicy) ([][]netmap.NodeInfo, error) {
	return b.vectors, nil
}

func testNode(v uint32) (n netmap.NodeInfo) {
	n.SetNetworkEndpoints("/ip4/0.0.0.0/tcp/" + strconv.Itoa(int(v)))

	return n
}

func copyVectors(v [][]netmap.NodeInfo) [][]netmap.NodeInfo {
	vc := make([][]netmap.NodeInfo, 0, len(v))

	for i := range v {
		ns := make([]netmap.NodeInfo, len(v[i]))
		copy(ns, v[i])

		vc = append(vc, ns)
	}

	return vc
}

func testPlacement(t *testing.T, ss, rs []int) ([][]netmap.NodeInfo, *container.Container) {
	nodes := make([][]netmap.NodeInfo, 0, len(rs))
	replicas := make([]netmap.ReplicaDescriptor, 0, len(rs))
	num := uint32(0)

	for i := range ss {
		ns := make([]netmap.NodeInfo, 0, ss[i])

		for j := 0; j < ss[i]; j++ {
			ns = append(ns, testNode(num))
			num++
		}

		nodes = append(nodes, ns)

		var rd netmap.ReplicaDescriptor
		rd.SetNumberOfObjects(uint32(rs[i]))

		replicas = append(replicas, rd)
	}

	policy := new(netmap.PlacementPolicy)
	policy.AddReplicas(replicas...)

	return nodes, container.New(container.WithPolicy(policy))
}

func assertSameAddress(t *testing.T, ni netmap.NodeInfo, addr network.AddressGroup) {
	var netAddr network.AddressGroup

	err := netAddr.FromIterator(netmapcore.Node(ni))
	require.NoError(t, err)
	require.True(t, netAddr.Intersects(addr))
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
				assertSameAddress(t, n, addrs[j].Addresses())
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

		var n network.AddressGroup

		err = n.FromIterator(netmapcore.Node(nodes[1][0]))
		require.NoError(t, err)

		require.Equal(t, []Node{{addresses: n}}, tr.Next())
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
					assertSameAddress(t, nodes[curVector][i+j], addrs[j].Addresses())
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
				vectors: [][]netmap.NodeInfo{{nodes[1][1]}}, // single node (local)
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
