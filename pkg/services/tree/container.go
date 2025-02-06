package tree

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
)

type containerCache struct {
	sync.Mutex
	nm  *netmapSDK.NetMap
	lru *simplelru.LRU[string, containerCacheItem]
}

func (c *containerCache) init(size int) {
	c.lru, _ = simplelru.NewLRU[string, containerCacheItem](size, nil) // no error, size is positive
}

type containerCacheItem struct {
	cnr   *container.Container
	local int
	nodes []netmapSDK.NodeInfo
}

const defaultContainerCacheSize = 10

// getContainerNodes returns nodes in the container and a position of local key in the list.
func (s *Service) getContainerNodes(cid cidSDK.ID) ([]netmapSDK.NodeInfo, int, error) {
	nm, err := s.nmSource.NetMap()
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
	} else if item, ok := s.containerCache.lru.Get(cidStr); ok {
		if item.cnr == cnr {
			s.containerCache.Unlock()
			return item.nodes, item.local, nil
		}
	}
	s.containerCache.Unlock()

	policy := cnr.Value.PlacementPolicy()

	cntNodes, err := nm.ContainerNodes(policy, cid)
	if err != nil {
		return nil, -1, err
	}

	nodes := placement.FlattenNodes(cntNodes)

	localPos := -1
	for i := range nodes {
		if bytes.Equal(nodes[i].PublicKey(), s.rawPub) {
			localPos = i
			break
		}
	}

	s.containerCache.Lock()
	s.containerCache.nm = nm
	s.containerCache.lru.Add(cidStr, containerCacheItem{
		cnr:   cnr,
		local: localPos,
		nodes: nodes,
	})
	s.containerCache.Unlock()

	return nodes, localPos, err
}
