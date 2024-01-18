package main

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// storagePolicyRes structures persistent storage policy application result for
// particular container and network map incl. error.
type storagePolicyRes struct {
	nodeSets [][]netmapsdk.NodeInfo
	err      error
}

type containerNodesCacheKey struct {
	epoch uint64
	cnr   cid.ID
}

// max number of container storage policy applications results cached by
// containerNodes.
const cachedContainerNodesNum = 1000

// containerNodes wraps NeoFS network state to apply container storage policies.
//
// Since policy application results are consistent for fixed container and
// network map, they could be cached. The containerNodes caches up to
// cachedContainerNodesNum LRU results.
type containerNodes struct {
	containers container.Source
	network    netmap.Source

	cache *lru.Cache[containerNodesCacheKey, storagePolicyRes]
}

func newContainerNodes(containers container.Source, network netmap.Source) (*containerNodes, error) {
	l, err := lru.New[containerNodesCacheKey, storagePolicyRes](cachedContainerNodesNum)
	if err != nil {
		return nil, fmt.Errorf("create LRU container node cache for one epoch: %w", err)
	}
	return &containerNodes{
		containers: containers,
		network:    network,
		cache:      l,
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

	cnrCtx := containerPolicyContext{id: cnrID, containers: x.containers, network: x.network}

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

// preserves context of storage policy processing for the particular container.
type containerPolicyContext struct {
	// static
	id         cid.ID
	containers container.Source
	network    netmap.Source
	// dynamic
	cnr *container.Container
}

// applyAtEpoch applies storage policy of container referenced by parameterized
// ID to the network map at the specified epoch. applyAtEpoch checks existing
// results in the cache and stores new results in it.
func (x *containerPolicyContext) applyAtEpoch(epoch uint64, cache *lru.Cache[containerNodesCacheKey, storagePolicyRes]) (storagePolicyRes, error) {
	cacheKey := containerNodesCacheKey{epoch, x.id}
	if result, ok := cache.Get(cacheKey); ok {
		return result, nil
	}
	var result storagePolicyRes
	var err error
	if x.cnr == nil {
		x.cnr, err = x.containers.Get(x.id)
		if err != nil {
			// non-persistent error => do not cache
			return result, fmt.Errorf("read container by ID: %w", err)
		}
	}
	networkMap, err := x.network.GetNetMapByEpoch(epoch)
	if err != nil {
		// non-persistent error => do not cache
		return result, fmt.Errorf("read network map by epoch: %w", err)
	}
	result.nodeSets, result.err = networkMap.ContainerNodes(x.cnr.Value.PlacementPolicy(), x.id)
	cache.Add(cacheKey, result)
	return result, nil
}

// getNodesForObject reads storage policy of the referenced container from the
// underlying container storage, reads network map at the specified epoch from
// the underlying storage, applies the storage policy to it and returns sorted
// lists of selected storage nodes along with the per-list numbers of primary
// object holders. Resulting slices must not be changed.
func (x *containerNodes) getNodesForObject(addr oid.Address) ([][]netmapsdk.NodeInfo, []uint, error) {
	epoch, err := x.network.Epoch()
	if err != nil {
		return nil, nil, fmt.Errorf("read current NeoFS epoch: %w", err)
	}
	cnrID := addr.Container()
	cnr, err := x.containers.Get(cnrID)
	if err != nil {
		return nil, nil, fmt.Errorf("read container by ID: %w", err)
	}
	networkMap, err := x.network.GetNetMapByEpoch(epoch)
	if err != nil {
		return nil, nil, fmt.Errorf("read network map at epoch #%d: %w", epoch, err)
	}

	policy := cnr.Value.PlacementPolicy()
	nodeLists, err := networkMap.ContainerNodes(policy, cnrID)
	if err != nil {
		return nil, nil, fmt.Errorf("apply container's storage policy to the network map at epoch #%d: %w", epoch, err)
	}
	if nodeLists, err = networkMap.PlacementVectors(nodeLists, addr.Object()); err != nil {
		return nil, nil, fmt.Errorf("sort container nodes from the network map at epoch #%d: %w", epoch, err)
	}
	if len(nodeLists) != policy.NumberOfReplicas() {
		return nil, nil, fmt.Errorf("invalid result of container's storage policy application to the network map at epoch #%d: "+
			"diff number of storage node lists (%d) and required replica descriptors (%d)", epoch, len(nodeLists), policy.NumberOfReplicas())
	}

	primaryCounts := make([]uint, len(nodeLists))
	for i := range nodeLists {
		if primaryCounts[i] = uint(policy.ReplicaNumberByIndex(i)); primaryCounts[i] > uint(len(nodeLists[i])) {
			return nil, nil, fmt.Errorf("invalid result of container's storage policy application to the network map at epoch #%d: "+
				"invalid storage node list #%d: number of nodes (%d) is less than minimum required by the container policy (%d)",
				epoch, i, len(nodeLists), policy.NumberOfReplicas())
		}
	}
	return nodeLists, primaryCounts, nil
}
