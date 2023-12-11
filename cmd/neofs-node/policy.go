package main

import (
	"fmt"
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
)

const cachedContainerNodesPerEpochNum = 1000

type nodeSetsWithError struct {
	ns  [][]netmapsdk.NodeInfo
	err error
}

type containerNodesAtEpoch struct {
	mtx sync.RWMutex
	lru simplelru.LRUCache[cid.ID, nodeSetsWithError]
}

type containerNodes struct {
	containers container.Source
	network    netmap.Source

	lastCurrentEpochMtx sync.Mutex
	lastCurrentEpoch    uint64

	curEpochCache, prevEpochCache *containerNodesAtEpoch
}

func newContainerNodes(containers container.Source, network netmap.Source) (*containerNodes, error) {
	lru, err := simplelru.NewLRU[cid.ID, nodeSetsWithError](cachedContainerNodesPerEpochNum, nil)
	if err != nil {
		// should never happen
		return nil, fmt.Errorf("create LRU cache for container nodes: %w", err)
	}
	return &containerNodes{
		containers:     containers,
		network:        network,
		curEpochCache:  &containerNodesAtEpoch{lru: lru},
		prevEpochCache: new(containerNodesAtEpoch),
	}, nil
}

func forEachNodePubKeyInSets(nodeSets [][]netmapsdk.NodeInfo, f func(pubKey []byte) bool) bool {
	for i := range nodeSets {
		for j := range nodeSets[i] {
			if !f(nodeSets[i][j].PublicKey()) {
				return false
			}
		}
	}
	return true
}

func (x *containerNodes) forEachContainerNodePublicKeyInLastTwoEpochs(cnrID cid.ID, f func(pubKey []byte) bool) error {
	curEpoch, err := x.network.Epoch()
	if err != nil {
		return fmt.Errorf("read current NeoFS epoch: %w", err)
	}

	var curEpochCache, prevEpochCache *containerNodesAtEpoch
	x.lastCurrentEpochMtx.Lock()
	switch {
	case curEpoch == x.lastCurrentEpoch-1:
		curEpochCache = x.prevEpochCache
	case curEpoch > x.lastCurrentEpoch:
		curLRU, err := simplelru.NewLRU[cid.ID, nodeSetsWithError](cachedContainerNodesPerEpochNum, nil)
		if err != nil {
			// should never happen
			x.lastCurrentEpochMtx.Unlock()
			return fmt.Errorf("create LRU cache for container nodes: %w", err)
		}

		if curEpoch == x.lastCurrentEpoch+1 {
			x.prevEpochCache = x.curEpochCache
		} else {
			prevLRU, err := simplelru.NewLRU[cid.ID, nodeSetsWithError](cachedContainerNodesPerEpochNum, nil)
			if err != nil {
				// should never happen
				x.lastCurrentEpochMtx.Unlock()
				return fmt.Errorf("create LRU cache for container nodes: %w", err)
			}
			x.prevEpochCache = &containerNodesAtEpoch{lru: prevLRU}
		}
		x.curEpochCache = &containerNodesAtEpoch{lru: curLRU}
		x.lastCurrentEpoch = curEpoch
		fallthrough
	case curEpoch == x.lastCurrentEpoch:
		curEpochCache = x.curEpochCache
		prevEpochCache = x.prevEpochCache
	}
	x.lastCurrentEpochMtx.Unlock()

	var cnr *container.Container

	processEpoch := func(cache *containerNodesAtEpoch, epoch uint64) (nodeSetsWithError, error) {
		var result nodeSetsWithError
		var isCached bool
		if cache != nil {
			cache.mtx.Lock()
			defer cache.mtx.Unlock()
			result, isCached = cache.lru.Get(cnrID)
		}
		if !isCached {
			if cnr == nil {
				var err error
				cnr, err = x.containers.Get(cnrID)
				if err != nil {
					// not persistent error => do not cache
					return result, fmt.Errorf("read container by ID: %w", err)
				}
			}
			networkMap, err := x.network.GetNetMapByEpoch(epoch)
			if err != nil {
				// not persistent error => do not cache
				return result, fmt.Errorf("read network map at epoch #%d: %w", epoch, err)
			}
			result.ns, result.err = networkMap.ContainerNodes(cnr.Value.PlacementPolicy(), cnrID)
			if cache != nil {
				cache.lru.Add(cnrID, result)
			}
		}
		return result, nil
	}

	cur, err := processEpoch(curEpochCache, curEpoch)
	if err != nil {
		return err
	}
	if cur.err == nil && (!forEachNodePubKeyInSets(cur.ns, f)) {
		return nil
	}
	if curEpoch == 0 {
		if cur.err != nil {
			return fmt.Errorf("select container nodes for current epoch #%d: %w", curEpoch, cur.err)
		}
		return nil
	}

	prev, err := processEpoch(prevEpochCache, curEpoch-1)
	if err != nil {
		if cur.err != nil {
			return fmt.Errorf("select container nodes for both epochs: (previous=%d) %w, (current=%d) %w",
				curEpoch-1, err, curEpoch, cur.err)
		}
		return err
	}
	if prev.err == nil && !forEachNodePubKeyInSets(prev.ns, f) {
		return nil
	}

	if cur.err != nil {
		if prev.err != nil {
			return fmt.Errorf("select container nodes for both epochs: (previous=%d) %w, (current=%d) %w",
				curEpoch-1, prev.err, curEpoch, cur.err)
		}
		return fmt.Errorf("select container nodes for current epoch #%d: %w", curEpoch, cur.err)
	}
	if prev.err != nil {
		return fmt.Errorf("select container nodes for previous epoch #%d: %w", curEpoch-1, prev.err)
	}
	return nil
}
