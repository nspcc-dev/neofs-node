package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

const anyEpoch = 42

type testContainer struct {
	id  cid.ID
	val container.Container
	err error

	calls []cid.ID
}

func (x testContainer) assertCalledNTimesWith(t testing.TB, n int, id cid.ID) {
	require.Len(t, x.calls, n)
	for i := range x.calls {
		require.Equal(t, id, x.calls[i])
	}
}

func (x *testContainer) Get(id cid.ID) (container.Container, error) {
	x.calls = append(x.calls, id)
	if id != x.id {
		return x.val, fmt.Errorf("unexpected container requested %s!=%s", id, x.id)
	}
	return x.val, x.err
}

type testNetwork struct {
	epoch    uint64
	epochErr error

	curNetmap     *netmap.NetMap
	curNetmapErr  error
	prevNetmap    *netmap.NetMap
	prevNetmapErr error

	callEpochCount int
	callsNetmap    []uint64
}

func (x *testNetwork) NetMap() (*netmap.NetMap, error) {
	panic("unexpected call")
}

func (x testNetwork) assertNetmapCalledNTimes(t testing.TB, n int, epoch uint64) {
	require.Len(t, x.callsNetmap, n)
	for i := range x.callsNetmap {
		require.EqualValues(t, epoch, x.callsNetmap[i])
	}
}

func (x *testNetwork) GetNetMapByEpoch(epoch uint64) (*netmap.NetMap, error) {
	x.callsNetmap = append(x.callsNetmap, epoch)
	if epoch == x.epoch {
		return x.curNetmap, x.curNetmapErr
	}
	if x.epoch > 0 && epoch == x.epoch-1 {
		return x.prevNetmap, x.prevNetmapErr
	}
	return nil, fmt.Errorf("unexpected epoch #%d requested", epoch)
}

func (x testNetwork) assertEpochCallCount(t testing.TB, n int) {
	require.EqualValues(t, x.callEpochCount, n)
}

func (x *testNetwork) Epoch() (uint64, error) {
	x.callEpochCount++
	return x.epoch, x.epochErr
}

func newNetmapWithContainer(tb testing.TB, nodeNum int, selected ...[]int) ([]netmap.NodeInfo, *netmap.NetMap, container.Container) {
	nodes := make([]netmap.NodeInfo, nodeNum)
	for i := range nodes {
		key := make([]byte, 33)
		_, _ = rand.Read(key)
		nodes[i].SetPublicKey(key)

		for j := range selected {
			for k := range selected[j] {
				if i == selected[j][k] {
					nodes[i].SetAttribute("attr"+strconv.Itoa(j), "true")
					break
				}
			}
		}
	}

	var networkMap netmap.NetMap
	networkMap.SetNodes(nodes)

	var sbRpl, sbSlc, sbFlt strings.Builder
	for i := range selected {
		sbFlt.WriteString(fmt.Sprintf("FILTER attr%d EQ true AS F%d\n", i, i))
		sbSlc.WriteString(fmt.Sprintf("SELECT %d FROM F%d AS S%d\n", len(selected[i]), i, i))
		sbRpl.WriteString(fmt.Sprintf("REP %d IN S%d\n", len(selected[i]), i))
	}
	var policy netmap.PlacementPolicy
	strPolicy := fmt.Sprintf("%sCBF 1\n%s%s", &sbRpl, &sbSlc, &sbFlt)
	require.NoError(tb, policy.DecodeString(strPolicy), strPolicy)

	nodeSets, err := networkMap.ContainerNodes(policy, cidtest.ID())
	require.NoError(tb, err)
	require.Len(tb, nodeSets, len(selected))
	for i := range selected {
		require.Len(tb, nodeSets[i], len(selected[i]), i)
		for j := range selected[i] {
			require.Contains(tb, nodeSets[i], nodes[selected[i][j]], [2]int{i, j})
		}
	}

	var cnr container.Container
	cnr.SetPlacementPolicy(policy)

	return nodes, &networkMap, cnr
}

func TestContainerNodes_ForEachContainerNodePublicKeyInLastTwoEpochs(t *testing.T) {
	const anyEpoch = 42
	anyCnr := cidtest.ID()
	failOnCall := func(tb testing.TB) func([]byte) bool {
		return func([]byte) bool {
			tb.Fatal("must not be called")
			return false
		}
	}

	t.Run("read current epoch", func(t *testing.T) {
		epochErr := errors.New("any epoch error")
		network := &testNetwork{epochErr: epochErr}
		ns, err := newContainerNodes(new(testContainer), network)
		require.NoError(t, err)

		for n := 1; n < 10; n++ {
			err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, failOnCall(t))
			require.ErrorIs(t, err, epochErr)
			require.EqualError(t, err, "read current NeoFS epoch: any epoch error")
			// such error must not be cached
			network.assertEpochCallCount(t, n)
		}
	})

	t.Run("read container failure", func(t *testing.T) {
		cnrErr := errors.New("any container error")
		cnrs := &testContainer{id: anyCnr, err: cnrErr}
		ns, err := newContainerNodes(cnrs, &testNetwork{epoch: anyEpoch})
		require.NoError(t, err)

		for n := 1; n < 10; n++ {
			err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, failOnCall(t))
			require.ErrorIs(t, err, cnrErr)
			require.EqualError(t, err, "select container nodes for current epoch #42: read container by ID: any container error")
			// such error must not be cached
			cnrs.assertCalledNTimesWith(t, n, anyCnr)
		}
	})

	t.Run("read current netmap failure", func(t *testing.T) {
		curNetmapErr := errors.New("any current netmap error")
		network := &testNetwork{epoch: anyEpoch, curNetmapErr: curNetmapErr}
		ns, err := newContainerNodes(&testContainer{id: anyCnr}, network)
		require.NoError(t, err)

		for n := 1; n <= 10; n++ {
			err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, failOnCall(t))
			require.ErrorIs(t, err, curNetmapErr)
			require.EqualError(t, err, "select container nodes for current epoch #42: read network map by epoch: any current netmap error")
			network.assertEpochCallCount(t, n)
			// such error must not be cached
			network.assertNetmapCalledNTimes(t, n, network.epoch)
		}
	})

	t.Run("zero current epoch", func(t *testing.T) {
		nodes, curNetmap, cnr := newNetmapWithContainer(t, 5, []int{1, 3})
		cnrs := &testContainer{id: anyCnr, val: cnr}
		network := &testNetwork{epoch: 0, curNetmap: curNetmap}
		ns, err := newContainerNodes(cnrs, network)
		require.NoError(t, err)

		for n := 1; n < 10; n++ {
			var calledKeys [][]byte
			err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
				calledKeys = append(calledKeys, pubKey)
				return true
			})
			require.NoError(t, err)
			require.Len(t, calledKeys, 2)
			require.Contains(t, calledKeys, nodes[1].PublicKey())
			require.Contains(t, calledKeys, nodes[3].PublicKey())
			network.assertEpochCallCount(t, n)
			// result is cached, no longer disturb the components
			cnrs.assertCalledNTimesWith(t, 1, anyCnr)
			network.assertNetmapCalledNTimes(t, 1, 0)
		}
	})

	t.Run("read previous network map failure", func(t *testing.T) {
		nodes, curNetmap, cnr := newNetmapWithContainer(t, 5, []int{1, 3})
		prevNetmapErr := errors.New("any previous netmap error")
		cnrs := &testContainer{id: anyCnr, val: cnr}
		network := &testNetwork{epoch: anyEpoch, curNetmap: curNetmap, prevNetmapErr: prevNetmapErr}

		ns, err := newContainerNodes(cnrs, network)
		require.NoError(t, err)

		for n := 1; n <= 10; n++ {
			var calledKeys [][]byte
			err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
				calledKeys = append(calledKeys, pubKey)
				return true
			})
			require.ErrorIs(t, err, prevNetmapErr)
			require.EqualError(t, err, "select container nodes for previous epoch #41: read network map by epoch: any previous netmap error")
			require.Len(t, calledKeys, 2)
			require.Contains(t, calledKeys, nodes[1].PublicKey())
			require.Contains(t, calledKeys, nodes[3].PublicKey())
			network.assertEpochCallCount(t, n)
			// previous epoch result not cached, so container requested each time
			cnrs.assertCalledNTimesWith(t, n, anyCnr)
			require.Len(t, network.callsNetmap, 1+n) // 1st time succeeds for current epoch
			require.EqualValues(t, network.epoch, network.callsNetmap[0])
			for _, e := range network.callsNetmap[1:] {
				require.EqualValues(t, network.epoch-1, e)
			}
		}
	})

	t.Run("apply policy failures", func(t *testing.T) {
		curNodes, curNetmap, cnr := newNetmapWithContainer(t, 5, []int{1, 3})
		prevNodes, prevNetmap, _ := newNetmapWithContainer(t, 5, []int{0, 4})
		failNetmap := new(netmap.NetMap)
		_, policyErr := failNetmap.ContainerNodes(cnr.PlacementPolicy(), anyCnr)
		require.Error(t, policyErr)

		t.Run("current OK, previous FAIL", func(t *testing.T) {
			cnrs := &testContainer{id: anyCnr, val: cnr}
			network := &testNetwork{epoch: anyEpoch, curNetmap: curNetmap, prevNetmap: failNetmap}
			ns, err := newContainerNodes(cnrs, network)
			require.NoError(t, err)

			for n := 1; n <= 10; n++ {
				var calledKeys [][]byte
				err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
					calledKeys = append(calledKeys, pubKey)
					return true
				})
				require.EqualError(t, err, fmt.Sprintf("select container nodes for previous epoch #41: %v", policyErr))
				require.Len(t, calledKeys, 2)
				require.Contains(t, calledKeys, curNodes[1].PublicKey())
				require.Contains(t, calledKeys, curNodes[3].PublicKey())
				network.assertEpochCallCount(t, n)
				// assert results are cached
				cnrs.assertCalledNTimesWith(t, 1, anyCnr)
				require.Len(t, network.callsNetmap, 2)
				require.EqualValues(t, network.epoch, network.callsNetmap[0])
				require.EqualValues(t, network.epoch-1, network.callsNetmap[1])
			}
		})
		t.Run("current FAIL w/o previous", func(t *testing.T) {
			cnrs := &testContainer{id: anyCnr, val: cnr}
			network := &testNetwork{epoch: anyEpoch, curNetmap: failNetmap, prevNetmap: prevNetmap}
			ns, err := newContainerNodes(cnrs, network)
			require.NoError(t, err)

			for n := 1; n <= 10; n++ {
				var calledKeys [][]byte
				err = ns.forEachContainerNode(anyCnr, false, func(node netmap.NodeInfo) bool {
					calledKeys = append(calledKeys, node.PublicKey())
					return true
				})
				require.EqualError(t, err, fmt.Sprintf("select container nodes for current epoch #42: %v", policyErr))
				require.Empty(t, calledKeys)
				// assert results are cached
				cnrs.assertCalledNTimesWith(t, 1, anyCnr)
				require.Len(t, network.callsNetmap, 1)
				require.EqualValues(t, network.epoch, network.callsNetmap[0])
			}
		})
		t.Run("current FAIL, previous OK", func(t *testing.T) {
			cnrs := &testContainer{id: anyCnr, val: cnr}
			network := &testNetwork{epoch: anyEpoch, curNetmap: failNetmap, prevNetmap: prevNetmap}
			ns, err := newContainerNodes(cnrs, network)
			require.NoError(t, err)

			for n := 1; n <= 10; n++ {
				var calledKeys [][]byte
				err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
					calledKeys = append(calledKeys, pubKey)
					return true
				})
				require.EqualError(t, err, fmt.Sprintf("select container nodes for current epoch #42: %v", policyErr))
				require.Len(t, calledKeys, 2)
				require.Contains(t, calledKeys, prevNodes[0].PublicKey())
				require.Contains(t, calledKeys, prevNodes[4].PublicKey())
				// assert results are cached
				cnrs.assertCalledNTimesWith(t, 1, anyCnr)
				require.Len(t, network.callsNetmap, 2)
				require.EqualValues(t, network.epoch, network.callsNetmap[0])
				require.EqualValues(t, network.epoch-1, network.callsNetmap[1])
			}
		})
		t.Run("current FAIL, previous not available", func(t *testing.T) {
			cnrs := &testContainer{id: anyCnr, val: cnr}
			network := &testNetwork{
				epoch:         anyEpoch,
				curNetmap:     failNetmap,
				prevNetmapErr: errors.New("any previous netmap error"),
			}
			ns, err := newContainerNodes(cnrs, network)
			require.NoError(t, err)

			for n := 1; n <= 10; n++ {
				var calledKeys [][]byte
				err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
					calledKeys = append(calledKeys, pubKey)
					return true
				})
				require.EqualError(t, err,
					fmt.Sprintf("select container nodes for both epochs: (current#42) %v; (previous#41) "+
						"read network map by epoch: any previous netmap error",
						policyErr))
				require.Empty(t, calledKeys)
				cnrs.assertCalledNTimesWith(t, n, anyCnr)
				require.Len(t, network.callsNetmap, 1+n) // current cached, previous not
				require.EqualValues(t, network.epoch, network.callsNetmap[0])
				require.EqualValues(t, network.epoch-1, network.callsNetmap[1])
			}
		})
		t.Run("fail for both epochs", func(t *testing.T) {
			cnrs := &testContainer{id: anyCnr, val: cnr}
			network := &testNetwork{epoch: anyEpoch, curNetmap: failNetmap, prevNetmap: failNetmap}
			ns, err := newContainerNodes(cnrs, network)
			require.NoError(t, err)

			for n := 1; n <= 10; n++ {
				var calledKeys [][]byte
				err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
					calledKeys = append(calledKeys, pubKey)
					return true
				})
				require.EqualError(t, err,
					fmt.Sprintf("select container nodes for both epochs: (current#42) %v; (previous#41) %v",
						policyErr, policyErr))
				require.Empty(t, calledKeys)
				// assert results are cached
				cnrs.assertCalledNTimesWith(t, 1, anyCnr)
				require.Len(t, network.callsNetmap, 2)
				require.EqualValues(t, network.epoch, network.callsNetmap[0])
				require.EqualValues(t, network.epoch-1, network.callsNetmap[1])
			}
		})
	})

	t.Run("both epochs OK", func(t *testing.T) {
		curNodes, curNetmap, cnr := newNetmapWithContainer(t, 5, []int{1, 3})
		prevNodes, prevNetmap, _ := newNetmapWithContainer(t, 5, []int{0, 4})
		cnrs := &testContainer{id: anyCnr, val: cnr}
		network := &testNetwork{epoch: anyEpoch, curNetmap: curNetmap, prevNetmap: prevNetmap}
		ns, err := newContainerNodes(cnrs, network)
		require.NoError(t, err)

		for n := 1; n <= 10; n++ {
			var calledKeys [][]byte
			err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
				calledKeys = append(calledKeys, pubKey)
				return true
			})
			require.NoError(t, err)
			require.Len(t, calledKeys, 4)
			require.Contains(t, calledKeys, curNodes[1].PublicKey())
			require.Contains(t, calledKeys, curNodes[3].PublicKey())
			require.Contains(t, calledKeys, prevNodes[0].PublicKey())
			require.Contains(t, calledKeys, prevNodes[4].PublicKey())
			cnrs.assertCalledNTimesWith(t, 1, anyCnr)
			require.Len(t, network.callsNetmap, 2)
			require.EqualValues(t, network.epoch, network.callsNetmap[0])
			require.EqualValues(t, network.epoch-1, network.callsNetmap[1])
		}
	})

	t.Run("interrupt", func(t *testing.T) {
		curNodes, curNetmap, cnr := newNetmapWithContainer(t, 5, []int{1, 3})
		prevNodes, prevNetmap, _ := newNetmapWithContainer(t, 5, []int{0, 4})
		curNodeKeys := [][]byte{curNodes[1].PublicKey(), curNodes[3].PublicKey()}
		prevNodeKeys := [][]byte{prevNodes[0].PublicKey(), prevNodes[4].PublicKey()}
		cnrs := &testContainer{id: anyCnr, val: cnr}
		network := &testNetwork{epoch: anyEpoch, curNetmap: curNetmap, prevNetmap: prevNetmap}
		ns, err := newContainerNodes(cnrs, network)
		require.NoError(t, err)

		for limit := 1; limit <= 4; limit++ {
			var calledKeys [][]byte
			err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
				calledKeys = append(calledKeys, pubKey)
				return len(calledKeys) < limit
			})
			require.NoError(t, err)
			require.Len(t, calledKeys, limit)
			switch limit {
			case 1:
				require.Contains(t, curNodeKeys, calledKeys[0])
			case 2:
				require.ElementsMatch(t, curNodeKeys, calledKeys[:2])
			case 3:
				require.ElementsMatch(t, curNodeKeys, calledKeys[:2])
				require.Contains(t, prevNodeKeys, calledKeys[2])
			case 4:
				require.ElementsMatch(t, curNodeKeys, calledKeys[:2])
				require.ElementsMatch(t, prevNodeKeys, calledKeys[2:])
			}
		}
	})

	t.Run("epoch switches", func(t *testing.T) {
		curNodes, curNetmap, cnr := newNetmapWithContainer(t, 5, []int{1, 3})
		prevNodes, prevNetmap, _ := newNetmapWithContainer(t, 5, []int{0, 4})
		newNodes1, newNetmap1, _ := newNetmapWithContainer(t, 6, []int{2, 5})
		newNodes2, newNetmap2, _ := newNetmapWithContainer(t, 6, []int{3, 4})
		call := func(ns *containerNodes) (res [][]byte) {
			err := ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
				res = append(res, pubKey)
				return true
			})
			require.NoError(t, err)
			return res
		}
		assertCall := func(cns *containerNodes, ns ...netmap.NodeInfo) {
			res := call(cns)
			require.Len(t, res, len(ns))
			for i := range ns {
				require.Contains(t, res, ns[i].PublicKey())
			}
		}
		for _, tc := range []struct {
			name            string
			changeEpoch     func(*uint64)
			newNetmaps      [2]*netmap.NetMap // current, previous
			selectedNodes   []netmap.NodeInfo
			extraReadNetmap []uint64 // current, previous
		}{
			{
				name:            "increment",
				changeEpoch:     func(e *uint64) { *e++ },
				newNetmaps:      [2]*netmap.NetMap{newNetmap1, curNetmap},
				selectedNodes:   []netmap.NodeInfo{newNodes1[2], newNodes1[5], curNodes[1], curNodes[3]},
				extraReadNetmap: []uint64{anyEpoch + 1},
			},
			{
				name:            "long jump forward",
				changeEpoch:     func(e *uint64) { *e += 10 },
				newNetmaps:      [2]*netmap.NetMap{newNetmap1, newNetmap2},
				selectedNodes:   []netmap.NodeInfo{newNodes1[2], newNodes1[5], newNodes2[3], newNodes2[4]},
				extraReadNetmap: []uint64{anyEpoch + 10, anyEpoch + 9},
			},
			{
				name:            "decrement",
				changeEpoch:     func(e *uint64) { *e-- },
				newNetmaps:      [2]*netmap.NetMap{prevNetmap, newNetmap1},
				selectedNodes:   []netmap.NodeInfo{prevNodes[0], prevNodes[4], newNodes1[2], newNodes1[5]},
				extraReadNetmap: []uint64{anyEpoch - 2},
			},
			{
				name:            "long jump backward",
				changeEpoch:     func(e *uint64) { *e -= 10 },
				newNetmaps:      [2]*netmap.NetMap{newNetmap1, newNetmap2},
				selectedNodes:   []netmap.NodeInfo{newNodes1[2], newNodes1[5], newNodes2[3], newNodes2[4]},
				extraReadNetmap: []uint64{anyEpoch - 10, anyEpoch - 11},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cnrs := &testContainer{id: anyCnr, val: cnr}
				network := &testNetwork{epoch: anyEpoch, curNetmap: curNetmap, prevNetmap: prevNetmap}
				ns, err := newContainerNodes(cnrs, network)
				require.NoError(t, err)

				assertCall(ns, curNodes[1], curNodes[3], prevNodes[0], prevNodes[4])
				cnrs.assertCalledNTimesWith(t, 1, anyCnr)
				require.Len(t, network.callsNetmap, 2)
				require.EqualValues(t, network.epoch, network.callsNetmap[0])
				require.EqualValues(t, network.epoch-1, network.callsNetmap[1])

				// update epoch
				tc.changeEpoch(&network.epoch)
				network.curNetmap, network.prevNetmap = tc.newNetmaps[0], tc.newNetmaps[1]

				assertCall(ns, tc.selectedNodes...)
				// one more container and netmap calls
				cnrs.assertCalledNTimesWith(t, 2, anyCnr)
				require.Len(t, network.callsNetmap, 2+len(tc.extraReadNetmap))
				require.Equal(t, tc.extraReadNetmap, network.callsNetmap[2:])
			})
		}
	})
}

func TestContainerNodes_GetNodesForObject(t *testing.T) {
	anyAddr := oidtest.Address()
	t.Run("read current epoch failure", func(t *testing.T) {
		epochErr := errors.New("any epoch error")
		network := &testNetwork{epochErr: epochErr}
		ns, err := newContainerNodes(new(testContainer), network)
		require.NoError(t, err)

		for n := 1; n < 10; n++ {
			_, _, _, err = ns.getNodesForObject(anyAddr)
			require.ErrorIs(t, err, epochErr)
			require.EqualError(t, err, "read current NeoFS epoch: any epoch error")
			// such error must not be cached
			network.assertEpochCallCount(t, n)
		}
	})
	t.Run("read container failure", func(t *testing.T) {
		cnrErr := errors.New("any container error")
		cnrs := &testContainer{id: anyAddr.Container(), err: cnrErr}
		ns, err := newContainerNodes(cnrs, &testNetwork{epoch: anyEpoch})
		require.NoError(t, err)

		for n := 1; n < 10; n++ {
			_, _, _, err = ns.getNodesForObject(anyAddr)
			require.ErrorIs(t, err, cnrErr)
			require.EqualError(t, err, "select container nodes for current epoch #42: read container by ID: any container error")
			// such error must not be cached
			cnrs.assertCalledNTimesWith(t, n, anyAddr.Container())
		}
	})
	t.Run("read netmap failure", func(t *testing.T) {
		curNetmapErr := errors.New("any current netmap error")
		network := &testNetwork{epoch: anyEpoch, curNetmapErr: curNetmapErr}
		ns, err := newContainerNodes(&testContainer{id: anyAddr.Container()}, network)
		require.NoError(t, err)

		for n := 1; n <= 10; n++ {
			_, _, _, err = ns.getNodesForObject(anyAddr)
			require.ErrorIs(t, err, curNetmapErr)
			require.EqualError(t, err, "select container nodes for current epoch #42: read network map by epoch: any current netmap error")
			network.assertEpochCallCount(t, n)
			// such error must not be cached
			network.assertNetmapCalledNTimes(t, n, network.epoch)
		}
	})
	t.Run("apply policy failures", func(t *testing.T) {
		t.Run("select container nodes", func(t *testing.T) {
			_, _, cnr := newNetmapWithContainer(t, 5, []int{1, 3})
			failNetmap := new(netmap.NetMap)
			_, policyErr := failNetmap.ContainerNodes(cnr.PlacementPolicy(), anyAddr.Container())
			require.Error(t, policyErr)

			cnrs := &testContainer{id: anyAddr.Container(), val: cnr}
			network := &testNetwork{epoch: anyEpoch, curNetmap: failNetmap}
			ns, err := newContainerNodes(cnrs, network)
			require.NoError(t, err)

			for n := 1; n <= 10; n++ {
				_, _, _, err = ns.getNodesForObject(anyAddr)
				require.EqualError(t, err, fmt.Sprintf("select container nodes for current epoch #42: %v", policyErr))
				network.assertEpochCallCount(t, n)
				// assert results are cached
				cnrs.assertCalledNTimesWith(t, 1, anyAddr.Container())
				require.Len(t, network.callsNetmap, 1)
				require.EqualValues(t, network.epoch, network.callsNetmap[0])
			}
		})
		t.Run("diff num of node lists and replica descriptors", func(t *testing.T) {
			_, networkMap, cnr := newNetmapWithContainer(t, 5, []int{1, 3}, []int{3, 4})
			cnrs := &testContainer{id: anyAddr.Container(), val: cnr}
			network := &testNetwork{epoch: anyEpoch, curNetmap: networkMap}
			ns, err := newContainerNodes(cnrs, network)
			require.NoError(t, err)
			ns.getContainerNodesFunc = func(nm netmap.NetMap, policy netmap.PlacementPolicy, cnrID cid.ID) ([][]netmap.NodeInfo, error) {
				require.Equal(t, *networkMap, nm)
				require.Equal(t, cnr.PlacementPolicy(), policy)
				require.Equal(t, anyAddr.Container(), cnrID)
				return make([][]netmap.NodeInfo, 4), nil
			}

			for n := 1; n <= 10; n++ {
				_, _, _, err = ns.getNodesForObject(anyAddr)
				require.EqualError(t, err, "select container nodes for current epoch #42: "+
					"invalid result of container's storage policy application to the network map: "+
					"diff number of storage node sets (4) and rules (2)")
				network.assertEpochCallCount(t, n)
				// assert results are cached
				cnrs.assertCalledNTimesWith(t, 1, anyAddr.Container())
				require.Len(t, network.callsNetmap, 1)
				require.EqualValues(t, network.epoch, network.callsNetmap[0])
			}
		})

		t.Run("not enough nodes in some list", func(t *testing.T) {
			_, networkMap, cnr := newNetmapWithContainer(t, 5, []int{1, 3}, []int{3, 4})
			cnrs := &testContainer{id: anyAddr.Container(), val: cnr}
			network := &testNetwork{epoch: anyEpoch, curNetmap: networkMap}
			ns, err := newContainerNodes(cnrs, network)
			require.NoError(t, err)
			ns.getContainerNodesFunc = func(nm netmap.NetMap, policy netmap.PlacementPolicy, cnrID cid.ID) ([][]netmap.NodeInfo, error) {
				require.Equal(t, *networkMap, nm)
				require.Equal(t, cnr.PlacementPolicy(), policy)
				require.Equal(t, anyAddr.Container(), cnrID)
				nodeLists, err := nm.ContainerNodes(policy, cnrID)
				require.NoError(t, err)
				res := slices.Clone(nodeLists)
				res[1] = res[1][:len(res[1])-1]
				return res, nil
			}

			for n := 1; n <= 10; n++ {
				_, _, _, err = ns.getNodesForObject(anyAddr)
				require.EqualError(t, err, "select container nodes for current epoch #42: "+
					"invalid result of container's storage policy application to the network map: "+
					"invalid storage node set #1: number of nodes (1) is less than minimum required by REP rule (2)")
				network.assertEpochCallCount(t, n)
				// assert results are cached
				cnrs.assertCalledNTimesWith(t, 1, anyAddr.Container())
				require.Len(t, network.callsNetmap, 1)
				require.EqualValues(t, network.epoch, network.callsNetmap[0])
			}
		})
		t.Run("diff num of node lists and replica descriptors", func(t *testing.T) {
			_, networkMap, cnr := newNetmapWithContainer(t, 5, []int{1, 3}, []int{3, 4})
			cnrs := &testContainer{id: anyAddr.Container(), val: cnr}
			network := &testNetwork{epoch: anyEpoch, curNetmap: networkMap}
			ns, err := newContainerNodes(cnrs, network)
			require.NoError(t, err)
			ns.getContainerNodesFunc = func(nm netmap.NetMap, policy netmap.PlacementPolicy, cnrID cid.ID) ([][]netmap.NodeInfo, error) {
				require.Equal(t, *networkMap, nm)
				require.Equal(t, cnr.PlacementPolicy(), policy)
				require.Equal(t, anyAddr.Container(), cnrID)
				return make([][]netmap.NodeInfo, 4), nil
			}

			for n := 1; n <= 10; n++ {
				_, _, _, err = ns.getNodesForObject(anyAddr)
				require.EqualError(t, err, "select container nodes for current epoch #42: "+
					"invalid result of container's storage policy application to the network map: "+
					"diff number of storage node sets (4) and rules (2)")
				network.assertEpochCallCount(t, n)
				// assert results are cached
				cnrs.assertCalledNTimesWith(t, 1, anyAddr.Container())
				require.Len(t, network.callsNetmap, 1)
				require.EqualValues(t, network.epoch, network.callsNetmap[0])
			}
		})

		t.Run("sort nodes failure", func(t *testing.T) {
			nodes, networkMap, cnr := newNetmapWithContainer(t, 5, []int{1, 3})
			cnrs := &testContainer{id: anyAddr.Container(), val: cnr}
			network := &testNetwork{epoch: anyEpoch, curNetmap: networkMap}
			ns, err := newContainerNodes(cnrs, network)
			require.NoError(t, err)
			ns.sortContainerNodesFunc = func(nm netmap.NetMap, ns [][]netmap.NodeInfo, id oid.ID) ([][]netmap.NodeInfo, error) {
				require.Equal(t, *networkMap, nm)
				require.Equal(t, anyAddr.Object(), id)
				for i := range ns {
					for j := range ns[i] {
						require.Contains(t, nodes, ns[i][j], [2]int{i, j})
					}
				}
				return nil, errors.New("any sort error")
			}

			for n := 1; n <= 10; n++ {
				_, _, _, err = ns.getNodesForObject(anyAddr)
				require.EqualError(t, err, "sort container nodes for object: any sort error")
				network.assertEpochCallCount(t, n)
				// assert results are cached
				cnrs.assertCalledNTimesWith(t, 1, anyAddr.Container())
				require.Len(t, network.callsNetmap, 1)
				require.EqualValues(t, network.epoch, network.callsNetmap[0])
			}
		})
	})
	t.Run("OK", func(t *testing.T) {
		nodes, networkMap, cnr := newNetmapWithContainer(t, 10, [][]int{
			{1, 3},
			{2, 4, 6},
			{5},
			{0, 1, 7, 8, 9},
		}...)
		cnrs := &testContainer{id: anyAddr.Container(), val: cnr}
		network := &testNetwork{epoch: anyEpoch, curNetmap: networkMap}
		ns, err := newContainerNodes(cnrs, network)
		require.NoError(t, err)

		for n := 1; n <= 10; n++ {
			nodeLists, primCounts, _, err := ns.getNodesForObject(anyAddr)
			require.NoError(t, err)
			require.Len(t, primCounts, 4)
			require.Len(t, nodeLists, 4)
			require.EqualValues(t, 2, primCounts[0])
			require.ElementsMatch(t, []netmap.NodeInfo{nodes[1], nodes[3]}, nodeLists[0])
			require.EqualValues(t, 3, primCounts[1])
			require.ElementsMatch(t, []netmap.NodeInfo{nodes[2], nodes[4], nodes[6]}, nodeLists[1])
			require.EqualValues(t, 1, primCounts[2])
			require.ElementsMatch(t, []netmap.NodeInfo{nodes[5]}, nodeLists[2])
			require.EqualValues(t, 5, primCounts[3])
			require.ElementsMatch(t, []netmap.NodeInfo{nodes[0], nodes[1], nodes[7], nodes[8], nodes[9]}, nodeLists[3])
			cnrs.assertCalledNTimesWith(t, 1, anyAddr.Container())
			require.Len(t, network.callsNetmap, 1)
			require.EqualValues(t, network.epoch, network.callsNetmap[0])
		}
	})
}
