package placement

import (
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type netMapBuilder struct {
	nmSrc netmap.Source
	// mtx protects lastNm and containerCache fields.
	mtx    sync.Mutex
	lastNm *netmapSDK.Netmap
	// containerCache caches container nodes by ID. It is used to skip `GetContainerNodes` invocation if
	// neither netmap nor container has changed.
	containerCache simplelru.LRUCache
}

type netMapSrc struct {
	netmap.Source

	nm *netmapSDK.Netmap
}

// defaultContainerCacheSize is the default size for the container cache.
const defaultContainerCacheSize = 10

func NewNetworkMapBuilder(nm *netmapSDK.Netmap) Builder {
	cache, _ := simplelru.NewLRU(defaultContainerCacheSize, nil) // no error
	return &netMapBuilder{
		nmSrc:          &netMapSrc{nm: nm},
		containerCache: cache,
	}
}

func NewNetworkMapSourceBuilder(nmSrc netmap.Source) Builder {
	cache, _ := simplelru.NewLRU(defaultContainerCacheSize, nil) // no error
	return &netMapBuilder{
		nmSrc:          nmSrc,
		containerCache: cache,
	}
}

func (s *netMapSrc) GetNetMap(diff uint64) (*netmapSDK.Netmap, error) {
	return s.nm, nil
}

func (b *netMapBuilder) BuildPlacement(cnr cid.ID, obj *oid.ID, p *netmapSDK.PlacementPolicy) ([]netmapSDK.Nodes, error) {
	nm, err := netmap.GetLatestNetworkMap(b.nmSrc)
	if err != nil {
		return nil, fmt.Errorf("could not get network map: %w", err)
	}

	binCnr := make([]byte, sha256.Size)
	cnr.Encode(binCnr)

	b.mtx.Lock()
	if nm == b.lastNm {
		raw, ok := b.containerCache.Get(string(binCnr))
		b.mtx.Unlock()
		if ok {
			cn := raw.(netmapSDK.ContainerNodes)
			return BuildObjectPlacement(nm, cn, obj)
		}
	} else {
		b.containerCache.Purge()
		b.mtx.Unlock()
	}

	cn, err := nm.GetContainerNodes(p, binCnr)
	if err != nil {
		return nil, fmt.Errorf("could not get container nodes: %w", err)
	}

	b.mtx.Lock()
	b.containerCache.Add(string(binCnr), cn)
	b.mtx.Unlock()

	return BuildObjectPlacement(nm, cn, obj)
}

func BuildObjectPlacement(nm *netmapSDK.Netmap, cnrNodes netmapSDK.ContainerNodes, id *oid.ID) ([]netmapSDK.Nodes, error) {
	if id == nil {
		return cnrNodes.Replicas(), nil
	}

	binObj := make([]byte, sha256.Size)
	id.Encode(binObj)

	on, err := nm.GetPlacementVectors(cnrNodes, binObj)
	if err != nil {
		return nil, fmt.Errorf("could not get placement vectors for object: %w", err)
	}

	return on, nil
}

// FlattenNodes appends each row to the flat list.
func FlattenNodes(ns []netmapSDK.Nodes) netmapSDK.Nodes {
	result := make(netmapSDK.Nodes, 0, len(ns))
	for i := range ns {
		result = append(result, ns[i]...)
	}

	return result
}
