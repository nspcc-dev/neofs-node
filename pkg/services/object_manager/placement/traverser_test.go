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

func testPlacement(t *testing.T, ss, rs []int) ([][]netmap.NodeInfo, container.Container) {
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

	var policy netmap.PlacementPolicy
	policy.SetReplicas(replicas)

	var cnr container.Container
	cnr.SetPlacementPolicy(policy)

	return nodes, cnr
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

func TestCopiesNumber(t *testing.T) {
	t.Run("copies_less_than_reps", func(t *testing.T) {
		selectors := []int{1, 2, 3} // 3 vectors with 1, 2, 3 lengths
		replicas := []int{0, 2, 3}  // REP 0 ... REP 2 ... REP 3

		nodes, cnr := testPlacement(t, selectors, replicas)
		nodesCopy := copyVectors(nodes)

		tr, err := NewTraverser(
			ForContainer(cnr),
			UseBuilder(&testBuilder{vectors: nodesCopy}),
			WithCopiesNumber(1),
		)
		require.NoError(t, err)

		// the first vector should be skipped; the second vector should
		// have only one node because of `WithCopiesNumber(1)`; after
		// submitting success submitting no additional nodes required

		addrs := tr.Next()
		require.Len(t, addrs, 1)

		tr.SubmitSuccess()

		addrs = tr.Next()
		require.Empty(t, addrs)
	})

	t.Run("copies_more_than_reps", func(t *testing.T) {
		selectors := []int{1, 1, 1} // 3 vectors with 1, 1, 1 lengths
		replicas := []int{1, 1, 1}  // REP 1 ... REP 1 ... REP 1

		nodes, cnr := testPlacement(t, selectors, replicas)
		nodesCopy := copyVectors(nodes)

		tr, err := NewTraverser(
			ForContainer(cnr),
			UseBuilder(&testBuilder{vectors: nodesCopy}),
			WithCopiesNumber(4), // 1 + 1 + 1 < 4
		)
		require.NoError(t, err)

		for _, rep := range replicas {
			addrs := tr.Next()
			require.Len(t, addrs, rep)

			tr.SubmitSuccess()
		}

		require.False(t, tr.Success())
	})

	t.Run("success_less_than_required", func(t *testing.T) {
		selectors := []int{10, 10, 10} // 3 vectors with 10, 10, 10 lengths
		replicas := []int{1, 2, 3}     // REP 1 ... REP 2 ... REP 3

		nodes, cnr := testPlacement(t, selectors, replicas)
		nodesCopy := copyVectors(nodes)

		tr, err := NewTraverser(
			ForContainer(cnr),
			UseBuilder(&testBuilder{vectors: nodesCopy}),
			WithCopiesNumber(1),
		)
		require.NoError(t, err)

		for addr := tr.Next(); addr != nil; addr = tr.Next() {
			// return 1 node from the 10 len vector
			// trying to satisfy the first REP descriptor
			// one by one
			require.Len(t, addr, 1)

			// but do not submit success
		}

		require.False(t, tr.Success())
	})
}
