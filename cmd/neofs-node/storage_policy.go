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

// Static cache settings related to storage policy processing by the storage node.
const (
	cachedEpochsWithContainerNodes = 2
	cacheSizeContainerNodesAtEpoch = 20
	cacheSizeObjectNodesAtEpoch    = 100
)

// result of applying storage policy of some NeoFS container to some NeoFS
// network map.
type containerNodes struct {
	primaryNodesNums []uint32
	// TODO: try storing indices of immutable list to reduce memory consumption
	//  see also https://github.com/nspcc-dev/neofs-sdk-go/issues/541
	nodeSets [][]netmapsdk.NodeInfo
}

func (x containerNodes) hasNodeWithPublicKey(bPubKey []byte) bool {
	for i := range x.nodeSets {
		for j := range x.nodeSets[i] {
			if bytes.Equal(x.nodeSets[i][j].PublicKey(), bPubKey) {
				return true
			}
		}
	}

	return false
}

type containerNodesBuildRes struct {
	mtx sync.RWMutex
	val containerNodes
	err error
}

type objectNodesBuildRes struct {
	mtx sync.RWMutex
	val [][]netmapsdk.NodeInfo
	err error
}

type containerNodesCacheItem struct {
	containerNodesBuildRes

	lruObjectsMtx sync.RWMutex
	lruObjects    *simplelru.LRU[oid.ID, *objectNodesBuildRes]
}

func newContainerNodesCacheItem() (*containerNodesCacheItem, error) {
	l, err := simplelru.NewLRU[oid.ID, *objectNodesBuildRes](cacheSizeObjectNodesAtEpoch, nil)
	if err != nil {
		return nil, fmt.Errorf("init LRU cache for per-object storage nodes: %w", err)
	}

	return &containerNodesCacheItem{
		lruObjects: l,
	}, nil
}

type containerNodesCache struct {
	lruMtx sync.RWMutex
	lru    *simplelru.LRU[cid.ID, *containerNodesCacheItem]
}

// TODO: invalidate removed containers?
type containerNodesBuilder struct {
	netmaps    netmap.Source
	containers container.Source

	curEpoch    uint64
	curEpochMtx sync.RWMutex

	cachesByEpoch [cachedEpochsWithContainerNodes]*containerNodesCache // values can be nil

	// callback for runtime failures of the in-memory LRU caches
	onInternalErr func(error)
}

func newContainerNodesCache() (*containerNodesCache, error) {
	l, err := simplelru.NewLRU[cid.ID, *containerNodesCacheItem](cacheSizeContainerNodesAtEpoch, nil)
	if err != nil {
		return nil, fmt.Errorf("init LRU cache for per-container storage nodes: %w", err)
	}

	return &containerNodesCache{
		lru: l,
	}, nil
}

func newContainerNodesBuilder(netmaps netmap.Source, containers container.Source, initEpoch uint64, onInternalErr func(error)) (*containerNodesBuilder, error) {
	var err error
	res := &containerNodesBuilder{
		netmaps:       netmaps,
		containers:    containers,
		curEpoch:      initEpoch,
		onInternalErr: onInternalErr,
	}

	for i := 0; i < len(res.cachesByEpoch); i++ {
		res.cachesByEpoch[i], err = newContainerNodesCache()
		if err != nil {
			return nil, fmt.Errorf("init cache for epoch %d: %w", initEpoch, err)
		}

		if initEpoch == 0 {
			break
		}

		initEpoch--
	}

	return res, nil
}

func (x *containerNodesBuilder) setCurrentEpoch(curEpoch uint64) {
	x.curEpochMtx.Lock()
	defer x.curEpochMtx.Unlock()

	if curEpoch <= x.curEpoch {
		return
	}

	diff := curEpoch - x.curEpoch // normally 1 is always expected
	if diff > cachedEpochsWithContainerNodes {
		diff = cachedEpochsWithContainerNodes
	}

	// TODO(issue): if netmap is unchanged => previous results remain (storage
	//  policies are immutable) => we can keep them for the current epoch
	copy(x.cachesByEpoch[diff:], x.cachesByEpoch[:])

	var err error
	for i := uint64(0); i < diff; i++ {
		x.cachesByEpoch[i], err = newContainerNodesCache()
		if err != nil {
			x.onInternalErr(fmt.Errorf("init cache for epoch %d: %w", curEpoch-diff, err))
		}
	}

	x.curEpoch = curEpoch
}

func (x *containerNodesBuilder) _getForEpochAndPolicy(cnrID cid.ID, epoch uint64, storagePolicyOpt *netmapsdk.PlacementPolicy) (containerNodes, *netmapsdk.NetMap, *containerNodesCacheItem, error) {
	var cache *containerNodesCache

	x.curEpochMtx.RLock()
	if epoch == 0 {
		epoch = x.curEpoch
	}
	cached := epoch <= x.curEpoch && epoch+cachedEpochsWithContainerNodes >= x.curEpoch
	if cached {
		cache = x.cachesByEpoch[x.curEpoch-epoch]
		if cache == nil {
			cached = false
		}
	}
	// unlock instantly to not block epoch switching (extremely important for write
	// operations). Even if the epoch instantly becomes too old to cache, the
	// overhead will be one write to a memory cache that is no longer needed
	x.curEpochMtx.RUnlock()

	// read network map and storage policy (below) without lock. The main reason is
	// potentially prolonged error caching leading to a false negative result.
	// Excessive value caching will complicate and reduce the manageability of
	// application caches (storages are cached themselves in practice)
	networkMap, err := x.netmaps.GetNetMapByEpoch(epoch)
	if err != nil {
		return containerNodes{}, nil, nil, fmt.Errorf("read network map at the epoch: %w", err)
	}

	var ok bool
	var cachedVal *containerNodesCacheItem
	if cached {
		cache = x.cachesByEpoch[x.curEpoch-epoch]

		cache.lruMtx.RLock()
		cachedVal, ok = cache.lru.Get(cnrID)
		cache.lruMtx.RUnlock()
		if ok {
			cachedVal.mtx.RLock()
			res, err := cachedVal.val, cachedVal.err
			cachedVal.mtx.RUnlock()

			return res, networkMap, cachedVal, err
		}
	}

	var storagePolicy netmapsdk.PlacementPolicy
	if storagePolicyOpt != nil {
		storagePolicy = *storagePolicyOpt
	} else {
		cnr, err := x.containers.Get(cnrID)
		if err != nil {
			return containerNodes{}, nil, nil, fmt.Errorf("read container by ID: %w", err)
		}

		storagePolicy = cnr.Value.PlacementPolicy()
	}

	if cached {
		cache.lruMtx.Lock()
		// cache was unlocked for some time, check again
		cachedVal, ok = cache.lru.Get(cnrID)
		if ok {
			// in this case, we wasted time reading the storage policy and network map, see
			// comment above for more on this
			cache.lruMtx.Unlock()

			cachedVal.mtx.RLock()
			res, err := cachedVal.val, cachedVal.err
			cachedVal.mtx.RUnlock()

			return res, networkMap, cachedVal, err
		}

		// notify other routines that the calculation is in progress asap in order to
		// reduce the chance of double calculation. Parallel computation is still
		// possible under a colossal load with a large spread of requested containers:
		// in this case, cached values can be evicted almost immediately. However, in
		// practice this is rarely expected, in contrast to high load on a small number
		// of containers. The last scenario will show better performance with the
		// current approach.
		cachedVal, err = newContainerNodesCacheItem()
		if err != nil {
			x.onInternalErr(err)
			cached = false
		} else {
			cachedVal.mtx.Lock()
			defer cachedVal.mtx.Unlock() // to prevent deadlock caused by panic

			cache.lru.Add(cnrID, cachedVal)
			cache.lruMtx.Unlock()
		}
	}

	var res containerNodes

	res.nodeSets, err = networkMap.ContainerNodes(storagePolicy, cnrID)
	if err != nil {
		err = fmt.Errorf("apply container's storage policy to the network map: %w", err)
		if cached {
			cachedVal.err = err
		}

		return containerNodes{}, nil, nil, err
	}

	res.primaryNodesNums = make([]uint32, storagePolicy.NumberOfReplicas())
	for i := range res.primaryNodesNums {
		res.primaryNodesNums[i] = storagePolicy.ReplicaNumberByIndex(i)
	}

	if cached {
		cachedVal.val = res
	}

	return res, networkMap, cachedVal, nil
}
func (x *containerNodesBuilder) _getForEpoch(cnrID cid.ID, epoch uint64) (containerNodes, *netmapsdk.NetMap, *containerNodesCacheItem, error) {
	return x._getForEpochAndPolicy(cnrID, epoch, nil)
}

func (x *containerNodesBuilder) getForEpoch(cnrID cid.ID, epoch uint64) (containerNodes, *netmapsdk.NetMap, error) {
	cnrNodes, networkMap, _, err := x._getForEpoch(cnrID, epoch)
	return cnrNodes, networkMap, err
}

func (x *containerNodesBuilder) getForObjectAtEpoch(addr oid.Address, epoch uint64) (containerNodes, error) {
	cnrNodes, networkMap, cache, err := x._getForEpoch(addr.Container(), epoch)
	if err != nil {
		return cnrNodes, err
	}

	cnrNodes.nodeSets, err = sortContainerNodesForObjectAtEpoch(cnrNodes.nodeSets, networkMap, cache, addr.Object())
	return cnrNodes, err
}

func sortContainerNodesForObjectAtEpoch(
	cnrNodeSets [][]netmapsdk.NodeInfo,
	networkMap *netmapsdk.NetMap,
	cache *containerNodesCacheItem,
	obj oid.ID,
) ([][]netmapsdk.NodeInfo, error) {
	var ok bool
	var cachedVal *objectNodesBuildRes
	if cache != nil {
		cache.lruObjectsMtx.RLock()
		cachedVal, ok = cache.lruObjects.Get(obj)
		cache.lruObjectsMtx.RUnlock()
		if ok {
			cachedVal.mtx.RLock()
			res, err := cachedVal.val, cachedVal.err
			cachedVal.mtx.RUnlock()

			return res, err
		}

		cache.lruObjectsMtx.Lock()
		// cache was unlocked for some time, check again
		cachedVal, ok = cache.lruObjects.Get(obj)
		if ok {
			cache.lruObjectsMtx.Unlock()

			cachedVal.mtx.RLock()
			res, err := cachedVal.val, cachedVal.err
			cachedVal.mtx.RUnlock()

			return res, err
		}

		cachedVal = new(objectNodesBuildRes)
		cachedVal.mtx.Lock()
		defer cachedVal.mtx.Unlock() // defer to prevent deadlock caused by panic

		cache.lruObjects.Add(obj, cachedVal)
		cache.lruObjectsMtx.Unlock()
	}

	res, err := networkMap.PlacementVectors(cnrNodeSets, obj)
	if err != nil {
		err = fmt.Errorf("sort container nodes for the object: %w", err)
		if cachedVal != nil {
			cachedVal.err = err
		}

		return nil, err
	}

	if cachedVal != nil {
		cachedVal.val = res
	}

	return res, nil
}
