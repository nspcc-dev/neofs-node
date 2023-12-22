package main

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// storage policy calculations are cached only for current and previous epochs
// because they are processed in 99.9% cases.
const cachedStoragePolicyEpochs = 2

type nodesError struct {
	// TODO: try storing indices of immutable list to reduce memory consumption
	//  see also https://github.com/nspcc-dev/neofs-sdk-go/issues/541
	nodes [][]netmapsdk.NodeInfo
	local int
	err   error
}

type storagePolicyApplicationCache struct {
	containersMtx sync.RWMutex
	containersLRU *simplelru.LRU[cid.ID, nodesError]

	objectsMtx sync.RWMutex
	objectsLRU *simplelru.LRU[oid.Address, nodesError]
}

func newStoragePolicyApplicationsCache() *storagePolicyApplicationCache {
	var res storagePolicyApplicationCache
	var err error
	// TODO(issue): explore adaptive size
	res.containersLRU, err = simplelru.NewLRU[cid.ID, nodesError](1000, nil)
	if err == nil {
		res.objectsLRU, err = simplelru.NewLRU[oid.Address, nodesError](10000, nil)
	}
	if err != nil {
		// should never happen: we construct a pure in-memory entity, error is returned
		// only to nonsense parameters e.g. non-positive size
		panic(fmt.Sprintf("unexpected LRU cache construction failure: %v", err))
	}

	return &res
}

type (
	isLocalPublicKeyFunc            = func([]byte) bool
	readContainerStoragePolicyFunc  = func(cid.ID) (netmapsdk.PlacementPolicy, error)
	readNetmapByEpochFunc           = func(epoch uint64) (*netmapsdk.NetMap, error)
	selectContainerNodesFunc        = func(*netmapsdk.NetMap, netmapsdk.PlacementPolicy, cid.ID) ([][]netmapsdk.NodeInfo, error)
	sortContainerNodesForObjectFunc = func(*netmapsdk.NetMap, [][]netmapsdk.NodeInfo, oid.ID) ([][]netmapsdk.NodeInfo, error)
)

type containerStoragePolicer struct {
	readContainerStoragePolicy  readContainerStoragePolicyFunc
	readNetmapByEpoch           readNetmapByEpochFunc
	isLocalPubKey               func([]byte) bool
	selectContainerNodes        selectContainerNodesFunc
	sortContainerNodesForObject sortContainerNodesForObjectFunc

	curEpochMtx sync.RWMutex
	curEpoch    uint64

	// current epoch, previous and so on
	caches [cachedStoragePolicyEpochs]*storagePolicyApplicationCache
}

func newContainerStoragePolicer(containers container.Source, netmaps netmap.Source, isLocalPubKey isLocalPublicKeyFunc) *containerStoragePolicer {
	return _newContainerStoragePolicer(func(id cid.ID) (netmapsdk.PlacementPolicy, error) {
		cnr, err := containers.Get(id)
		if err != nil {
			return netmapsdk.PlacementPolicy{}, fmt.Errorf("read container by ID: %w", err)
		}

		return cnr.Value.PlacementPolicy(), nil
	}, netmaps.GetNetMapByEpoch, isLocalPubKey, (*netmapsdk.NetMap).ContainerNodes, (*netmapsdk.NetMap).PlacementVectors)
}

func _newContainerStoragePolicer(
	containers readContainerStoragePolicyFunc,
	netmaps readNetmapByEpochFunc,
	isLocalPubKey isLocalPublicKeyFunc,
	selectContainerNodes selectContainerNodesFunc,
	sortContainerNodesForObject sortContainerNodesForObjectFunc,
) *containerStoragePolicer {
	res := &containerStoragePolicer{
		readContainerStoragePolicy:  containers,
		readNetmapByEpoch:           netmaps,
		isLocalPubKey:               isLocalPubKey,
		selectContainerNodes:        selectContainerNodes,
		sortContainerNodesForObject: sortContainerNodesForObject,
	}

	for i := range res.caches {
		// TODO(issue): consider resizing. Normally, current epoch is processed, the
		//  previous ones are used much less frequently in practice. By narrowing the
		//  cache size of the past, we can cache more of the present.
		//
		// note that it's ok to construct caches for all epochs even though they haven't
		// been requested yet since newStoragePolicyApplicationsCache does not allocate full space
		// for the cache items. Also, it makes code a bit simpler.
		res.caches[i] = newStoragePolicyApplicationsCache()
	}

	return res
}

func (x *containerStoragePolicer) setCurrentEpoch(curEpoch uint64) {
	x.curEpochMtx.Lock()
	defer x.curEpochMtx.Unlock()

	if curEpoch < x.curEpoch { // just in case
		return
	}

	diff := curEpoch - x.curEpoch // in practice 1 is always expected
	if diff > cachedStoragePolicyEpochs {
		diff = cachedStoragePolicyEpochs
	}

	tail := x.caches[cachedStoragePolicyEpochs-diff:]
	copy(x.caches[diff:], x.caches[:])
	copy(x.caches[:], tail)

	// TODO(issue): if netmap is unchanged => previous results remain (storage policies are
	//  immutable) => we can keep them for the current epoch
	for i := range tail {
		tail[i].containersLRU.Purge()
		tail[i].objectsLRU.Purge()
	}

	x.curEpoch = curEpoch
}

func (x *containerStoragePolicer) ForEachRemoteContainerNode(cnrID cid.ID, startEpoch, nPast uint64, f func(netmapsdk.NodeInfo) bool) error {
	x.curEpochMtx.RLock()
	defer x.curEpochMtx.RUnlock()

	processed := make(map[int]struct{})

	process := func(nodes []netmapsdk.NodeInfo, nodeIndexSets [][]int, localIndex int) bool {
		for i := range nodeIndexSets {
			for j := range nodeIndexSets[i] {
				if _, ok := processed[nodeIndexSets[i][j]]; !ok {
					if nodeIndexSets[i][j] != localIndex && !f(nodes[nodeIndexSets[i][j]]) {
						return false
					}
					processed[nodeIndexSets[i][j]] = struct{}{}
				}
			}
		}
		return true
	}

	var err error
	var epoch uint64
	var cache *storagePolicyApplicationCache
	var networkMap *netmapsdk.NetMap
	var nodeSets [][]netmapsdk.NodeInfo
	var storagePolicyRead bool
	var storagePolicy netmapsdk.PlacementPolicy

	for i := uint64(0); i <= nPast && i <= startEpoch; i++ {
		cache = nil
		epoch = startEpoch - i
		if epoch <= x.curEpoch {
			diff := x.curEpoch - epoch
			if diff < uint64(len(x.caches)) {
				cache = x.caches[diff]

				cache.mtx.RLock()
				nodeIndexSets, ok := cache.lru.Get(cnrID)
				cache.mtx.RUnlock()
				if ok {
					if nodeIndexSets.err != nil {
						return nodeIndexSets.err
					} else if !process(cache.netmap.Nodes(), nodeIndexSets.inds, nodeIndexSets.local) {
						return nil
					}
					continue
				}
			}
		}

		networkMap = nil

		if cache != nil {
			cache.mtx.Lock()
			// check cache again: it was unlocked for some time
			nodeIndexSets, ok := cache.lru.Get(cnrID)
			if ok {
				cache.mtx.Unlock()
				if nodeIndexSets.err != nil {
					return nodeIndexSets.err
				} else if !process(cache.netmap.Nodes(), nodeIndexSets.inds, nodeIndexSets.local) {
					return nil
				}
				continue
			}

			networkMap = cache.netmap
		}

		if networkMap == nil {
			networkMap, err = x.readNetmapByEpoch(epoch)
			if err != nil {
				if cache != nil {
					cache.mtx.Unlock()
				}
				// TODO(issue): if 404 => continue
				return fmt.Errorf("read network map for the epoch #%d: %w", epoch, err)
			}

			if cache != nil {
				// cache network map so all its results be consistent
				cache.netmap = networkMap
			}
		}

		if !storagePolicyRead {
			storagePolicy, err = x.readContainerStoragePolicy(cnrID)
			if err != nil {
				if cache != nil {
					cache.mtx.Unlock()
				}
				return fmt.Errorf("read storage policy of the container: %w", err)
			}

			storagePolicyRead = true
		}

		nodeSets, err = x.selectContainerNodes(networkMap, storagePolicy, cnrID)
		if err != nil {
			err = fmt.Errorf("apply container's storage policy to network map at the epoch #%d: %w", epoch, err)
			if cache != nil {
				cache.lru.Add(cnrID, nodesError{err: err})
				cache.mtx.Unlock()
			}
			// TODO: some errors allow to continue?
			return err
		}

		if cache != nil {
			indexLocal := -1
			indexSets := make([][]int, len(nodeSets))
			netmapNodes := cache.netmap.Nodes()
			for i := range indexSets {
				indexSets[i] = make([]int, len(nodeSets[i]))
			nextNode:
				for j := range nodeSets[i] {
					for k := range netmapNodes {
						bNodePubKey := netmapNodes[k].PublicKey()
						if indexLocal < 0 && x.isLocalPubKey(bNodePubKey) {
							indexLocal = k
						}
						if bytes.Equal(bNodePubKey, nodeSets[i][j].PublicKey()) {
							indexSets[i][j] = k
							continue nextNode
						}
					}

					// TODO: this is fatal actually
					err = fmt.Errorf("unidentified node as a result of applying container's storage policy to network map at the epoch #%d: %w",
						epoch, err)
					cache.lru.Add(cnrID, nodesError{err: err})
					cache.mtx.Unlock()
					return err
				}
			}

			cache.lru.Add(cnrID, nodesError{nodes: indexSets, local: indexLocal})
			cache.mtx.Unlock()

			if !process(netmapNodes, indexSets, indexLocal) {
				return nil
			}
		}

		for i := range nodeSets {
			for j := range nodeSets[i] {
				if !f(nodeSets[i][j]) {
					return nil
				}
			}
		}
	}

	return nil
}
