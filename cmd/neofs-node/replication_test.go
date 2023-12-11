package main

import (
	"errors"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var staticNodeKey = []byte("any binary key")

func getStaticNodeKey() []byte {
	return staticNodeKey
}

func getOtherNodeKey() []byte {
	res := slice.Copy(staticNodeKey)
	res[0]++
	return res
}

func getStaticEpoch() uint64 {
	return 1
}

func noCallNetmap(epoch uint64) (netmap.NetMap, error) {
	panic("must not be called")
}

func anyContainerStoragePolicy(cid.ID) (_ netmap.PlacementPolicy, _ error) {
	return
}

func somePolicyWithNodes() (p netmap.PlacementPolicy, match, mismatch netmap.NodeInfo) {
	const attrKey = "any_attr"
	const matchVal = "any_val"

	const filterName = "any filter name"
	var f netmap.Filter
	f.Equal(attrKey, matchVal)
	f.SetName(filterName)

	p.AddFilters(f)

	const selectorName = "any selector name"
	var s netmap.Selector
	s.SetNumberOfNodes(1)
	s.SelectByBucketAttribute(attrKey)
	s.SetFilterName(filterName)
	s.SetName(selectorName)

	p.AddSelectors(s)

	var r netmap.ReplicaDescriptor
	r.SetSelectorName(selectorName)
	r.SetNumberOfObjects(1)

	p.AddReplicas(r)

	match.SetAttribute(attrKey, matchVal)
	mismatch.SetAttribute(attrKey, matchVal+"bad")

	return
}

func TestReplicationNode_CompliesContainerStoragePolicy(t *testing.T) {
	var noCallPutSvc *putsvc.Service

	t.Run("read storage policy failure", func(t *testing.T) {
		testErr := errors.New("any error for test")

		node := newReplicationNode(zap.NewNop(), noCallPutSvc, getStaticNodeKey, func(id cid.ID) (netmap.PlacementPolicy, error) {
			return netmap.PlacementPolicy{}, testErr
		}, getStaticEpoch, noCallNetmap)

		_, err := node.CompliesContainerStoragePolicy(cidtest.ID())
		require.ErrorIs(t, err, testErr)
	})

	t.Run("read latest network map failure", func(t *testing.T) {
		testErr := errors.New("any error for test")

		node := newReplicationNode(zap.NewNop(), noCallPutSvc, getStaticNodeKey, anyContainerStoragePolicy, getStaticEpoch, func(epoch uint64) (netmap.NetMap, error) {
			require.EqualValues(t, getStaticEpoch(), epoch)
			return netmap.NetMap{}, testErr
		})

		_, err := node.CompliesContainerStoragePolicy(cidtest.ID())
		require.ErrorIs(t, err, testErr)
	})

	t.Run("local node outside network map", func(t *testing.T) {
		otherNodeKey := getOtherNodeKey()

		var n netmap.NodeInfo
		n.SetPublicKey(otherNodeKey)

		var nm netmap.NetMap
		nm.SetNodes([]netmap.NodeInfo{n})

		node := newReplicationNode(zap.NewNop(), noCallPutSvc, getStaticNodeKey, func(id cid.ID) (_ netmap.PlacementPolicy, _ error) {
			return
		}, getStaticEpoch, func(epoch uint64) (netmap.NetMap, error) {
			require.EqualValues(t, getStaticEpoch(), epoch)
			return nm, nil
		})

		res, err := node.CompliesContainerStoragePolicy(cidtest.ID())
		require.NoError(t, err)
		require.False(t, res)
	})

	t.Run("local node mismatches storage policy", func(t *testing.T) {
		cnr := cidtest.ID()
		policy, matchingNode, mismatchingNode := somePolicyWithNodes()

		matchingNode.SetPublicKey(getOtherNodeKey())
		mismatchingNode.SetPublicKey(getStaticNodeKey())

		var nm netmap.NetMap
		nm.SetNodes([]netmap.NodeInfo{matchingNode, mismatchingNode})

		node := newReplicationNode(zap.NewNop(), noCallPutSvc, getStaticNodeKey, func(id cid.ID) (netmap.PlacementPolicy, error) {
			require.Equal(t, cnr, id)
			return policy, nil
		}, getStaticEpoch, func(epoch uint64) (netmap.NetMap, error) {
			require.EqualValues(t, getStaticEpoch(), epoch)
			return nm, nil
		})

		res, err := node.CompliesContainerStoragePolicy(cnr)
		require.NoError(t, err)
		require.False(t, res)
	})

	t.Run("OK", func(t *testing.T) {
		cnr := cidtest.ID()
		policy, matchingNode, mismatchingNode := somePolicyWithNodes()

		matchingNode.SetPublicKey(getStaticNodeKey())

		var nm netmap.NetMap
		nm.SetNodes([]netmap.NodeInfo{matchingNode, mismatchingNode})

		node := newReplicationNode(zap.NewNop(), noCallPutSvc, getStaticNodeKey, func(id cid.ID) (netmap.PlacementPolicy, error) {
			require.Equal(t, cnr, id)
			return policy, nil
		}, getStaticEpoch, func(epoch uint64) (netmap.NetMap, error) {
			require.EqualValues(t, getStaticEpoch(), epoch)
			return nm, nil
		})

		res, err := node.CompliesContainerStoragePolicy(cnr)
		require.NoError(t, err)
		require.True(t, res)
	})
}

func TestReplicationNode_ClientCompliesContainerStoragePolicy(t *testing.T) {
	var noCallPutSvc *putsvc.Service

	t.Run("read storage policy failure", func(t *testing.T) {
		testErr := errors.New("any error for test")

		node := newReplicationNode(zap.NewNop(), noCallPutSvc, getStaticNodeKey, func(id cid.ID) (netmap.PlacementPolicy, error) {
			return netmap.PlacementPolicy{}, testErr
		}, getStaticEpoch, noCallNetmap)

		_, err := node.ClientCompliesContainerStoragePolicy(getOtherNodeKey(), cidtest.ID())
		require.ErrorIs(t, err, testErr)
	})

	t.Run("read latest network map failure", func(t *testing.T) {
		testErr := errors.New("any error for test")

		node := newReplicationNode(zap.NewNop(), noCallPutSvc, getStaticNodeKey, anyContainerStoragePolicy, getStaticEpoch, func(epoch uint64) (netmap.NetMap, error) {
			require.EqualValues(t, getStaticEpoch(), epoch)
			return netmap.NetMap{}, testErr
		})

		_, err := node.ClientCompliesContainerStoragePolicy(getOtherNodeKey(), cidtest.ID())
		require.ErrorIs(t, err, testErr)
	})

	t.Run("local node outside latest network map + previous fails", func(t *testing.T) {
		testErr := errors.New("any error for test")
		clientKey := getOtherNodeKey()

		var n netmap.NodeInfo
		n.SetPublicKey(getStaticNodeKey())

		var nm netmap.NetMap
		nm.SetNodes([]netmap.NodeInfo{n})

		node := newReplicationNode(zap.NewNop(), noCallPutSvc, getStaticNodeKey, anyContainerStoragePolicy, getStaticEpoch, func(epoch uint64) (netmap.NetMap, error) {
			if epoch < getStaticEpoch() {
				return netmap.NetMap{}, testErr
			}
			return nm, nil
		})

		_, err := node.ClientCompliesContainerStoragePolicy(clientKey, cidtest.ID())
		require.ErrorIs(t, err, testErr)
	})

	t.Run("local node mismatches storage policy with both network maps", func(t *testing.T) {
		cnr := cidtest.ID()
		policy, matchingNode, mismatchingNode := somePolicyWithNodes()
		clientKey := getOtherNodeKey()

		matchingNode.SetPublicKey(getStaticNodeKey())
		mismatchingNode.SetPublicKey(clientKey)

		var nmLatest netmap.NetMap
		nmLatest.SetNodes([]netmap.NodeInfo{matchingNode, mismatchingNode})

		var nmPrev netmap.NetMap
		nmPrev.SetNodes([]netmap.NodeInfo{matchingNode, mismatchingNode})

		node := newReplicationNode(zap.NewNop(), noCallPutSvc, getStaticNodeKey, func(id cid.ID) (netmap.PlacementPolicy, error) {
			require.Equal(t, cnr, id)
			return policy, nil
		}, getStaticEpoch, func(epoch uint64) (netmap.NetMap, error) {
			if epoch < getStaticEpoch() {
				return nmPrev, nil
			}
			return nmLatest, nil
		})

		res, err := node.ClientCompliesContainerStoragePolicy(clientKey, cnr)
		require.NoError(t, err)
		require.False(t, res)
	})

	t.Run("OK", func(t *testing.T) {
		cnr := cidtest.ID()
		policy, matchingNode, mismatchingNode := somePolicyWithNodes()
		clientKey := getOtherNodeKey()

		matchingNode.SetPublicKey(getStaticNodeKey())
		mismatchingNode.SetPublicKey(clientKey)

		var nmLatest netmap.NetMap
		nmLatest.SetNodes([]netmap.NodeInfo{matchingNode, mismatchingNode})

		node := newReplicationNode(zap.NewNop(), noCallPutSvc, getStaticNodeKey, func(id cid.ID) (netmap.PlacementPolicy, error) {
			require.Equal(t, cnr, id)
			return policy, nil
		}, getStaticEpoch, func(epoch uint64) (netmap.NetMap, error) {
			if epoch < getStaticEpoch() {
				var nm netmap.NetMap
				matchingNode.SetPublicKey(clientKey)
				mismatchingNode.SetPublicKey(getStaticNodeKey())
				nm.SetNodes([]netmap.NodeInfo{mismatchingNode, matchingNode})
				return nm, nil
			}
			return nmLatest, nil
		})

		res, err := node.ClientCompliesContainerStoragePolicy(clientKey, cnr)
		require.NoError(t, err)
		require.True(t, res)
	})
}
