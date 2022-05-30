package tree

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
)

type containerCache struct {
	sync.Mutex
	nm  *netmapSDK.Netmap
	lru *simplelru.LRU
}

func (c *containerCache) init() {
	c.lru, _ = simplelru.NewLRU(defaultContainerCacheSize, nil) // no error, size is positive
}

type containerCacheItem struct {
	cnr   *containerSDK.Container
	local int
	nodes netmapSDK.Nodes
}

const defaultContainerCacheSize = 10

// getContainerNodes returns nodes in the container and a position of local key in the list.
func (s *Service) getContainerNodes(cid cidSDK.ID) (netmapSDK.Nodes, int, error) {
	nm, err := s.nmSource.GetNetMap(0)
	if err != nil {
		return nil, -1, fmt.Errorf("can't get netmap: %w", err)
	}

	cnr, err := s.cnrSource.Get(cid)
	if err != nil {
		return nil, -1, fmt.Errorf("can't get container: %w", err)
	}

	cidStr := cid.String()

	s.containerCache.Lock()
	if s.containerCache.nm != nm {
		s.containerCache.lru.Purge()
	} else if v, ok := s.containerCache.lru.Get(cidStr); ok {
		item := v.(containerCacheItem)
		if item.cnr == cnr {
			s.containerCache.Unlock()
			return item.nodes, item.local, nil
		}
	}
	s.containerCache.Unlock()

	policy := cnr.PlacementPolicy()

	rawCID := make([]byte, sha256.Size)
	cid.Encode(rawCID)

	nodes, err := nm.GetContainerNodes(policy, rawCID)
	if err != nil {
		return nil, -1, err
	}

	ns := nodes.Flatten()
	localPos := -1
	for i := range ns {
		if bytes.Equal(ns[i].PublicKey(), s.rawPub) {
			localPos = i
			break
		}
	}

	s.containerCache.Lock()
	s.containerCache.nm = nm
	s.containerCache.lru.Add(cidStr, containerCacheItem{
		cnr:   cnr,
		local: localPos,
		nodes: ns,
	})
	s.containerCache.Unlock()

	return ns, localPos, err
}
