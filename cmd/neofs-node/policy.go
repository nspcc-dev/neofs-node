package main

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	sdkcontainer "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// storagePolicyRes structures persistent storage policy application result for
// particular container and network map incl. error.
type storagePolicyRes struct {
	nodeSets  [][]netmapsdk.NodeInfo
	repCounts []uint
	ecRules   []iec.Rule
	err       error
}

type (
	containerNodesCacheKey struct {
		epoch uint64
		cnr   cid.ID
	}
	objectNodesCacheKey struct {
		epoch uint64
		addr  oid.Address
	}
)

const (
	// max number of container storage policy applications results cached by
	// containerNodes.
	cachedContainerNodesNum = 1000
	// max number of object storage policy applications results cached by
	// containerNodes.
	cachedObjectNodesNum = 10000
)

type (
	getContainerNodesFunc  = func(netmapsdk.NetMap, netmapsdk.PlacementPolicy, cid.ID) ([][]netmapsdk.NodeInfo, error)
	sortContainerNodesFunc = func(netmapsdk.NetMap, [][]netmapsdk.NodeInfo, oid.ID) ([][]netmapsdk.NodeInfo, error)
)

// containerNodes wraps NeoFS network state to apply container storage policies.
//
// Since policy application results are consistent for fixed container and
// network map, they could be cached. The containerNodes caches up to
// cachedContainerNodesNum LRU results.
type containerNodes struct {
	containers container.Source
	network    netmap.Source

	cache    *lru.Cache[containerNodesCacheKey, storagePolicyRes]
	objCache *lru.Cache[objectNodesCacheKey, storagePolicyRes]

	// for testing
	getContainerNodesFunc  getContainerNodesFunc
	sortContainerNodesFunc sortContainerNodesFunc
}

func newContainerNodes(containers container.Source, network netmap.Source) (*containerNodes, error) {
	l, err := lru.New[containerNodesCacheKey, storagePolicyRes](cachedContainerNodesNum)
	if err != nil {
		return nil, fmt.Errorf("create LRU container node cache for one epoch: %w", err)
	}
	lo, err := lru.New[objectNodesCacheKey, storagePolicyRes](cachedObjectNodesNum)
	if err != nil {
		return nil, fmt.Errorf("create LRU container node cache for objects: %w", err)
	}
	return &containerNodes{
		containers:             containers,
		network:                network,
		cache:                  l,
		objCache:               lo,
		getContainerNodesFunc:  netmapsdk.NetMap.ContainerNodes,
		sortContainerNodesFunc: netmapsdk.NetMap.PlacementVectors,
	}, nil
}

// forEachContainerNodePublicKeyInLastTwoEpochs passes binary-encoded public key
// of each node match the referenced container's storage policy at two latest
// epochs into f. When f returns false, nil is returned instantly.
func (x *containerNodes) forEachContainerNodePublicKeyInLastTwoEpochs(cnrID cid.ID, f func(pubKey []byte) bool) error {
	return x.forEachContainerNode(cnrID, true, func(node netmapsdk.NodeInfo) bool {
		return f(node.PublicKey())
	})
}

func (x *containerNodes) forEachContainerNode(cnrID cid.ID, withPrevEpoch bool, f func(netmapsdk.NodeInfo) bool) error {
	curEpoch, err := x.network.Epoch()
	if err != nil {
		return fmt.Errorf("read current NeoFS epoch: %w", err)
	}

	cnrCtx := containerPolicyContext{id: cnrID, containers: x.containers, network: x.network, getNodesFunc: x.getContainerNodesFunc}

	resCur, err := cnrCtx.applyAtEpoch(curEpoch, x.cache)
	if err != nil {
		return fmt.Errorf("select container nodes for current epoch #%d: %w", curEpoch, err)
	} else if resCur.err == nil { // error case handled below
		for i := range resCur.nodeSets {
			for j := range resCur.nodeSets[i] {
				if !f(resCur.nodeSets[i][j]) {
					return nil
				}
			}
		}
	}

	if !withPrevEpoch || curEpoch == 0 {
		if resCur.err != nil {
			return fmt.Errorf("select container nodes for current epoch #%d: %w", curEpoch, resCur.err)
		}
		return nil
	}

	resPrev, err := cnrCtx.applyAtEpoch(curEpoch-1, x.cache)
	if err != nil {
		if resCur.err != nil {
			return fmt.Errorf("select container nodes for both epochs: (current#%d) %w; (previous#%d) %w",
				curEpoch, resCur.err, curEpoch-1, err)
		}
		return fmt.Errorf("select container nodes for previous epoch #%d: %w", curEpoch-1, err)
	} else if resPrev.err == nil { // error case handled below
		for i := range resPrev.nodeSets {
			for j := range resPrev.nodeSets[i] {
				if !f(resPrev.nodeSets[i][j]) {
					return nil
				}
			}
		}
	}

	if resCur.err != nil {
		if resPrev.err != nil {
			return fmt.Errorf("select container nodes for both epochs: (current#%d) %w; (previous#%d) %w",
				curEpoch, resCur.err, curEpoch-1, resPrev.err)
		}
		return fmt.Errorf("select container nodes for current epoch #%d: %w", curEpoch, resCur.err)
	} else if resPrev.err != nil {
		return fmt.Errorf("select container nodes for previous epoch #%d: %w", curEpoch-1, resPrev.err)
	}
	return nil
}

// getNodesForObject reads storage policy of the referenced container from the
// underlying container storage, reads network map at the specified epoch from
// the underlying storage, applies the storage policy to it and returns sorted
// lists of selected storage nodes along with the per-list numbers of primary
// object holders. Resulting slices must not be changed.
func (x *containerNodes) getNodesForObject(addr oid.Address) ([][]netmapsdk.NodeInfo, []uint, []iec.Rule, error) {
	curEpoch, err := x.network.Epoch()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read current NeoFS epoch: %w", err)
	}
	cacheKey := objectNodesCacheKey{curEpoch, addr}
	res, ok := x.objCache.Get(cacheKey)
	if ok {
		return res.nodeSets, res.repCounts, res.ecRules, res.err
	}
	cnrRes, networkMap, err := x.getForCurrentEpoch(curEpoch, addr.Container())
	if err != nil {
		return nil, nil, nil, err
	}
	if networkMap == nil {
		if networkMap, err = x.network.GetNetMapByEpoch(curEpoch); err != nil {
			// non-persistent error => do not cache
			return nil, nil, nil, fmt.Errorf("read network map by epoch: %w", err)
		}
	}
	res.repCounts = cnrRes.repCounts
	res.ecRules = cnrRes.ecRules
	res.nodeSets, res.err = x.sortContainerNodesFunc(*networkMap, cnrRes.nodeSets, addr.Object())
	if res.err != nil {
		res.err = fmt.Errorf("sort container nodes for object: %w", res.err)
	}
	x.objCache.Add(cacheKey, res)
	return res.nodeSets, res.repCounts, res.ecRules, res.err
}

func (x *containerNodes) getForCurrentEpoch(curEpoch uint64, cnr cid.ID) (storagePolicyRes, *netmapsdk.NetMap, error) {
	policy, networkMap, err := (&containerPolicyContext{
		id:           cnr,
		containers:   x.containers,
		network:      x.network,
		getNodesFunc: x.getContainerNodesFunc,
	}).applyToNetmap(curEpoch, x.cache)
	if err != nil || policy.err != nil {
		if err == nil {
			err = policy.err // cached in x.cache, no need to store in x.objCache
		}
		return storagePolicyRes{}, nil, fmt.Errorf("select container nodes for current epoch #%d: %w", curEpoch, err)
	}
	return policy, networkMap, nil
}

// preserves context of storage policy processing for the particular container.
type containerPolicyContext struct {
	// static
	id           cid.ID
	containers   container.Source
	network      netmap.Source
	getNodesFunc getContainerNodesFunc
	// dynamic
	cnr *sdkcontainer.Container
}

// applyAtEpoch applies storage policy of container referenced by parameterized
// ID to the network map at the specified epoch. applyAtEpoch checks existing
// results in the cache and stores new results in it.
func (x *containerPolicyContext) applyAtEpoch(epoch uint64, cache *lru.Cache[containerNodesCacheKey, storagePolicyRes]) (storagePolicyRes, error) {
	res, _, err := x.applyToNetmap(epoch, cache)
	return res, err
}

// applyToNetmap applies storage policy of container referenced by parameterized
// ID to the network map at the specified epoch. applyAtEpoch checks existing
// results in the cache and stores new results in it. Network map is returned if
// it was requested, i.e. on cache miss only.
func (x *containerPolicyContext) applyToNetmap(epoch uint64, cache *lru.Cache[containerNodesCacheKey, storagePolicyRes]) (storagePolicyRes, *netmapsdk.NetMap, error) {
	cacheKey := containerNodesCacheKey{epoch, x.id}
	if result, ok := cache.Get(cacheKey); ok {
		return result, nil, nil
	}
	var result storagePolicyRes
	var err error
	if x.cnr == nil {
		cnr, err := x.containers.Get(x.id)
		if err != nil {
			// non-persistent error => do not cache
			return result, nil, fmt.Errorf("read container by ID: %w", err)
		}
		x.cnr = &cnr
	}
	networkMap, err := x.network.GetNetMapByEpoch(epoch)
	if err != nil {
		// non-persistent error => do not cache
		return result, nil, fmt.Errorf("read network map by epoch: %w", err)
	}
	policy := x.cnr.PlacementPolicy()
	result.nodeSets, result.err = x.getNodesFunc(*networkMap, policy, x.id)
	if result.err == nil {
		// ContainerNodes should control following, but still better to double-check
		if policyNum := policy.NumberOfReplicas(); len(result.nodeSets) != policyNum {
			result.err = fmt.Errorf("invalid result of container's storage policy application to the network map: "+
				"diff number of storage node sets (%d) and required replica descriptors (%d)",
				len(result.nodeSets), policyNum)
		} else {
			result.repCounts = make([]uint, len(result.nodeSets))
			for i := range result.nodeSets {
				if result.repCounts[i] = uint(policy.ReplicaNumberByIndex(i)); result.repCounts[i] > uint(len(result.nodeSets[i])) {
					result.err = fmt.Errorf("invalid result of container's storage policy application to the network map: "+
						"invalid storage node set #%d: number of nodes (%d) is less than minimum required by the container policy (%d)",
						i, len(result.nodeSets[i]), result.repCounts[i])
					break
				}
			}
		}
	}
	cache.Add(cacheKey, result)
	return result, networkMap, nil
}
