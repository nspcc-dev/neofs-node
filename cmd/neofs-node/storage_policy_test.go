package main

import (
	"errors"
	"testing"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	netmaptest "github.com/nspcc-dev/neofs-sdk-go/netmap/test"
	"github.com/stretchr/testify/require"
)

func TestContainerStoragePolicer_ForEachRemoteContainerNode(t *testing.T) {
	const anyEpoch = 13
	anyCnr := cidtest.ID()
	nopIsLocalPubKey := func([]byte) bool { return false }

	t.Run("read netmap failure", func(t *testing.T) {
		var errReadNetmap = errors.New("any read netmap error")

		p := _newContainerStoragePolicer(func(id cid.ID) (netmap.PlacementPolicy, error) {
			require.Equal(t, anyCnr, id)
			return netmap.PlacementPolicy{}, apistatus.ErrContainerNotFound
		}, func(epoch uint64) (*netmap.NetMap, error) {
			require.EqualValues(t, anyEpoch, epoch)
			return nil, errReadNetmap
		}, nopIsLocalPubKey, (*netmap.NetMap).ContainerNodes)

		called := false
		f := func(info netmap.NodeInfo) bool {
			called = true
			return false
		}

		err := p.ForEachRemoteContainerNode(anyCnr, anyEpoch, 0, f)
		require.ErrorIs(t, err, errReadNetmap)
		require.False(t, called)

		err = p.ForEachRemoteContainerNode(anyCnr, anyEpoch, 0, f)
		require.ErrorIs(t, err, errReadNetmap)
		require.False(t, called)
	})

	t.Run("read container storage policy failure", func(t *testing.T) {
		var anyNetmap netmap.NetMap
		anyNetmap.SetNodes([]netmap.NodeInfo{netmaptest.NodeInfo(), netmaptest.NodeInfo()})
		var errReadPolicy = errors.New("any read container storage policy error")

		p := _newContainerStoragePolicer(func(id cid.ID) (netmap.PlacementPolicy, error) {
			require.Equal(t, anyCnr, id)
			return netmap.PlacementPolicy{}, errReadPolicy
		}, func(epoch uint64) (*netmap.NetMap, error) {
			require.EqualValues(t, anyEpoch, epoch)
			return &anyNetmap, nil
		}, nopIsLocalPubKey, (*netmap.NetMap).ContainerNodes)

		called := false
		f := func(info netmap.NodeInfo) bool {
			called = true
			return false
		}

		err := p.ForEachRemoteContainerNode(anyCnr, anyEpoch, 0, f)
		require.ErrorIs(t, err, errReadPolicy)
		require.False(t, called)

		err = p.ForEachRemoteContainerNode(anyCnr, anyEpoch, 0, f)
		require.ErrorIs(t, err, errReadPolicy)
		require.False(t, called)
	})

	t.Run("apply container storage policy failure", func(t *testing.T) {
		var anyNetmap netmap.NetMap
		anyNetmap.SetNodes([]netmap.NodeInfo{netmaptest.NodeInfo(), netmaptest.NodeInfo()})
		anyPolicy := netmaptest.PlacementPolicy()
		var errApplyPolicy = errors.New("any apply container storage policy error")
		applyPolicyCallCounter := 0

		p := _newContainerStoragePolicer(func(id cid.ID) (netmap.PlacementPolicy, error) {
			require.Equal(t, anyCnr, id)
			return anyPolicy, nil
		}, func(epoch uint64) (*netmap.NetMap, error) {
			require.EqualValues(t, anyEpoch, epoch)
			return &anyNetmap, nil
		}, nopIsLocalPubKey, func(netMap *netmap.NetMap, policy netmap.PlacementPolicy, id cid.ID) ([][]netmap.NodeInfo, error) {
			require.Equal(t, anyNetmap, *netMap)
			require.Equal(t, anyPolicy, policy)
			require.Equal(t, anyCnr, id)
			applyPolicyCallCounter++
			return nil, errApplyPolicy
		})
		p.setCurrentEpoch(anyEpoch)

		called := false
		f := func(info netmap.NodeInfo) bool {
			called = true
			return false
		}

		err := p.ForEachRemoteContainerNode(anyCnr, anyEpoch, 0, f)
		require.ErrorIs(t, err, errApplyPolicy)
		require.False(t, called)
		require.EqualValues(t, 1, applyPolicyCallCounter)

		err = p.ForEachRemoteContainerNode(anyCnr, anyEpoch, 0, f)
		require.ErrorIs(t, err, errApplyPolicy)
		require.False(t, called)
		require.EqualValues(t, 1, applyPolicyCallCounter) // result should be cached
	})

	t.Run("apply container storage policy failure", func(t *testing.T) {
		var anyNetmap netmap.NetMap
		anyNetmap.SetNodes([]netmap.NodeInfo{netmaptest.NodeInfo(), netmaptest.NodeInfo()})
		anyPolicy := netmaptest.PlacementPolicy()
		var errApplyPolicy = errors.New("any apply container storage policy error")
		applyPolicyCallCounter := 0

		p := _newContainerStoragePolicer(func(id cid.ID) (netmap.PlacementPolicy, error) {
			require.Equal(t, anyCnr, id)
			return anyPolicy, nil
		}, func(epoch uint64) (*netmap.NetMap, error) {
			require.EqualValues(t, anyEpoch, epoch)
			return &anyNetmap, nil
		}, nopIsLocalPubKey, func(netMap *netmap.NetMap, policy netmap.PlacementPolicy, id cid.ID) ([][]netmap.NodeInfo, error) {
			require.Equal(t, anyNetmap, *netMap)
			require.Equal(t, anyPolicy, policy)
			require.Equal(t, anyCnr, id)
			applyPolicyCallCounter++
			return nil, errApplyPolicy
		})
		p.setCurrentEpoch(anyEpoch)

		called := false
		f := func(info netmap.NodeInfo) bool {
			called = true
			return false
		}

		err := p.ForEachRemoteContainerNode(anyCnr, anyEpoch, 0, f)
		require.ErrorIs(t, err, errApplyPolicy)
		require.False(t, called)
		require.EqualValues(t, 1, applyPolicyCallCounter)

		err = p.ForEachRemoteContainerNode(anyCnr, anyEpoch, 0, f)
		require.ErrorIs(t, err, errApplyPolicy)
		require.False(t, called)
		require.EqualValues(t, 1, applyPolicyCallCounter) // result should be cached
	})

	t.Run("broken apply storage policy function", func(t *testing.T) {
		var anyNetmap netmap.NetMap
		anyNetmap.SetNodes([]netmap.NodeInfo{netmaptest.NodeInfo(), netmaptest.NodeInfo()})
		anyPolicy := netmaptest.PlacementPolicy()
		applyPolicyCallCounter := 0

		p := _newContainerStoragePolicer(func(id cid.ID) (netmap.PlacementPolicy, error) {
			require.Equal(t, anyCnr, id)
			return anyPolicy, nil
		}, func(epoch uint64) (*netmap.NetMap, error) {
			require.EqualValues(t, anyEpoch, epoch)
			return &anyNetmap, nil
		}, nopIsLocalPubKey, func(netMap *netmap.NetMap, policy netmap.PlacementPolicy, id cid.ID) ([][]netmap.NodeInfo, error) {
			require.Equal(t, anyNetmap, *netMap)
			require.Equal(t, anyPolicy, policy)
			require.Equal(t, anyCnr, id)
			applyPolicyCallCounter++

			return [][]netmap.NodeInfo{{netmaptest.NodeInfo()}}, nil
		})
		p.setCurrentEpoch(anyEpoch)

		called := false
		f := func(info netmap.NodeInfo) bool {
			called = true
			return false
		}

		err := p.ForEachRemoteContainerNode(anyCnr, anyEpoch, 0, f)
		require.ErrorContains(t, err, "unidentified node")
		require.False(t, called)
		require.EqualValues(t, 1, applyPolicyCallCounter)

		err = p.ForEachRemoteContainerNode(anyCnr, anyEpoch, 0, f)
		require.ErrorContains(t, err, "unidentified node")
		require.False(t, called)
		require.EqualValues(t, 1, applyPolicyCallCounter) // result should be cached
	})
}

func BenchmarkContainerStoragePolicer_ForEachRemoteContainerNode(b *testing.B) {
	cnr := cidtest.ID()
	const curEpoch = 13
	const lookupDepth = 4
	storagePolicy := netmaptest.PlacementPolicy()
	nodes := [][]netmap.NodeInfo{
		{netmaptest.NodeInfo(), netmaptest.NodeInfo()},
		{netmaptest.NodeInfo(), netmaptest.NodeInfo()},
	}
	var netMap netmap.NetMap
	netMap.SetNodes(append(nodes[0], nodes[1]...))

	p := _newContainerStoragePolicer(func(cid.ID) (netmap.PlacementPolicy, error) {
		return storagePolicy, nil
	}, func(uint64) (*netmap.NetMap, error) {
		return &netMap, nil
	}, func([]byte) bool {
		return false
	}, func(*netmap.NetMap, netmap.PlacementPolicy, cid.ID) ([][]netmap.NodeInfo, error) {
		return nodes, nil
	})
	p.setCurrentEpoch(curEpoch)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := p.ForEachRemoteContainerNode(cnr, curEpoch, lookupDepth, func(netmap.NodeInfo) bool {
			return true
		})
		require.NoError(b, err)
	}
}
