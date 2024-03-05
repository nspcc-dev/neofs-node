package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"testing"

	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

type testContainer struct {
	id  cid.ID
	val container.Container
	err error
}

func (x *testContainer) Get(id cid.ID) (*containercore.Container, error) {
	if !id.Equals(x.id) {
		return nil, fmt.Errorf("unexpected container requested %s!=%s", id, x.id)
	}
	if x.err != nil {
		return nil, x.err
	}
	return &containercore.Container{Value: x.val}, nil
}

type testNetwork struct {
	epoch    uint64
	epochErr error

	curNetmap     *netmap.NetMap
	curNetmapErr  error
	prevNetmap    *netmap.NetMap
	prevNetmapErr error
}

func (x *testNetwork) GetNetMap(diff uint64) (*netmap.NetMap, error) {
	panic("unexpected call")
}

func (x *testNetwork) GetNetMapByEpoch(epoch uint64) (*netmap.NetMap, error) {
	if epoch == x.epoch {
		return x.curNetmap, x.curNetmapErr
	}
	if x.epoch > 0 && epoch == x.epoch-1 {
		return x.prevNetmap, x.prevNetmapErr
	}
	return nil, fmt.Errorf("unexpected epoch #%d requested", epoch)
}

func (x *testNetwork) Epoch() (uint64, error) {
	return x.epoch, x.epochErr
}

func newNetmapWithContainer(tb testing.TB, nodeNum int, selected []int) ([]netmap.NodeInfo, *netmap.NetMap, container.Container) {
	nodes := make([]netmap.NodeInfo, nodeNum)
nextNode:
	for i := range nodes {
		key := make([]byte, 33)
		_, err := rand.Read(key)
		require.NoError(tb, err)
		nodes[i].SetPublicKey(key)

		for j := range selected {
			if i == selected[j] {
				nodes[i].SetAttribute("attr", "true")
				continue nextNode
			}
		}

		nodes[i].SetAttribute("attr", "false")
	}

	var networkMap netmap.NetMap
	networkMap.SetNodes(nodes)

	var policy netmap.PlacementPolicy
	strPolicy := fmt.Sprintf("REP %d CBF 1 SELECT %d FROM F FILTER attr EQ true AS F", len(selected), len(selected))
	require.NoError(tb, policy.DecodeString(strPolicy))

	nodeSets, err := networkMap.ContainerNodes(policy, cidtest.ID())
	require.NoError(tb, err)
	require.Len(tb, nodeSets, 1)
	require.Len(tb, nodeSets[0], len(selected))
	for i := range selected {
		require.Contains(tb, nodeSets[0], nodes[selected[i]], i)
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
		ns, err := newContainerNodes(new(testContainer), &testNetwork{epochErr: epochErr})
		require.NoError(t, err)

		err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, failOnCall(t))
		require.ErrorIs(t, err, epochErr)
	})

	t.Run("read container failure", func(t *testing.T) {
		cnrErr := errors.New("any container error")
		ns, err := newContainerNodes(&testContainer{
			id:  anyCnr,
			err: cnrErr,
		}, &testNetwork{epoch: anyEpoch})
		require.NoError(t, err)

		err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, failOnCall(t))
		require.ErrorIs(t, err, cnrErr)
	})

	t.Run("read current netmap failure", func(t *testing.T) {
		curNetmapErr := errors.New("any current netmap error")
		ns, err := newContainerNodes(&testContainer{id: anyCnr}, &testNetwork{
			epoch:        anyEpoch,
			curNetmapErr: curNetmapErr,
		})
		require.NoError(t, err)

		err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, failOnCall(t))
		require.ErrorIs(t, err, curNetmapErr)
	})

	t.Run("zero current epoch", func(t *testing.T) {
		nodes, curNetmap, cnr := newNetmapWithContainer(t, 5, []int{1, 3})

		ns, err := newContainerNodes(&testContainer{id: anyCnr, val: cnr}, &testNetwork{
			epoch:     0,
			curNetmap: curNetmap,
		})
		require.NoError(t, err)

		var calledKeys [][]byte
		err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
			calledKeys = append(calledKeys, pubKey)
			return true
		})
		require.NoError(t, err)
		require.Len(t, calledKeys, 2)
		require.Contains(t, calledKeys, nodes[1].PublicKey())
		require.Contains(t, calledKeys, nodes[3].PublicKey())
	})

	t.Run("zero current epoch", func(t *testing.T) {
		nodes, curNetmap, cnr := newNetmapWithContainer(t, 5, []int{1, 3})

		ns, err := newContainerNodes(&testContainer{id: anyCnr, val: cnr}, &testNetwork{
			epoch:     0,
			curNetmap: curNetmap,
		})
		require.NoError(t, err)

		var calledKeys [][]byte
		err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
			calledKeys = append(calledKeys, pubKey)
			return true
		})
		require.NoError(t, err)
		require.Len(t, calledKeys, 2)
		require.Contains(t, calledKeys, nodes[1].PublicKey())
		require.Contains(t, calledKeys, nodes[3].PublicKey())
	})

	t.Run("read previous network map failure", func(t *testing.T) {
		nodes, curNetmap, cnr := newNetmapWithContainer(t, 5, []int{1, 3})
		prevNetmapErr := errors.New("any previous netmap error")

		ns, err := newContainerNodes(&testContainer{id: anyCnr, val: cnr}, &testNetwork{
			epoch:         anyEpoch,
			curNetmap:     curNetmap,
			prevNetmapErr: prevNetmapErr,
		})
		require.NoError(t, err)

		var calledKeys [][]byte
		err = ns.forEachContainerNodePublicKeyInLastTwoEpochs(anyCnr, func(pubKey []byte) bool {
			calledKeys = append(calledKeys, pubKey)
			return true
		})
		require.ErrorIs(t, err, prevNetmapErr)
		require.Len(t, calledKeys, 2)
		require.Contains(t, calledKeys, nodes[1].PublicKey())
		require.Contains(t, calledKeys, nodes[3].PublicKey())
	})

	t.Run("both epochs OK", func(t *testing.T) {
		curNodes, curNetmap, cnr := newNetmapWithContainer(t, 5, []int{1, 3})
		prevNodes, prevNetmap, _ := newNetmapWithContainer(t, 5, []int{0, 4})

		ns, err := newContainerNodes(&testContainer{id: anyCnr, val: cnr}, &testNetwork{
			epoch:      anyEpoch,
			curNetmap:  curNetmap,
			prevNetmap: prevNetmap,
		})
		require.NoError(t, err)

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
	})
}
