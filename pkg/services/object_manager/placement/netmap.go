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
	lastNm *netmapSDK.NetMap
	// containerCache caches container nodes by ID. It is used to skip `GetContainerNodes` invocation if
	// neither netmap nor container has changed.
	containerCache simplelru.LRUCache
}

type netMapSrc struct {
	netmap.Source

	nm *netmapSDK.NetMap
}

// defaultContainerCacheSize is the default size for the container cache.
const defaultContainerCacheSize = 10

func NewNetworkMapBuilder(nm *netmapSDK.NetMap) Builder {
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

func (s *netMapSrc) GetNetMap(diff uint64) (*netmapSDK.NetMap, error) {
	return s.nm, nil
}

func (b *netMapBuilder) BuildPlacement(cnr cid.ID, obj *oid.ID, p netmapSDK.PlacementPolicy) ([][]netmapSDK.NodeInfo, error) {
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
			cn := raw.([][]netmapSDK.NodeInfo)
			return BuildObjectPlacement(nm, cn, obj)
		}
	} else {
		b.containerCache.Purge()
		b.mtx.Unlock()
	}

	cn, err := nm.ContainerNodes(p, binCnr)
	if err != nil {
		return nil, fmt.Errorf("could not get container nodes: %w", err)
	}

	b.mtx.Lock()
	b.containerCache.Add(string(binCnr), cn)
	b.mtx.Unlock()

	return BuildObjectPlacement(nm, cn, obj)
}

func BuildObjectPlacement(nm *netmapSDK.NetMap, cnrNodes [][]netmapSDK.NodeInfo, id *oid.ID) ([][]netmapSDK.NodeInfo, error) {
	if id == nil {
		return cnrNodes, nil
	}

	binObj := make([]byte, sha256.Size)
	id.Encode(binObj)

	on, err := nm.PlacementVectors(cnrNodes, binObj)
	if err != nil {
		return nil, fmt.Errorf("could not get placement vectors for object: %w", err)
	}

	return on, nil
}

// FlattenNodes appends each row to the flat list.
func FlattenNodes(ns [][]netmapSDK.NodeInfo) []netmapSDK.NodeInfo {
	var sz int

	for i := range ns {
		sz += len(ns[i])
	}

	result := make([]netmapSDK.NodeInfo, 0, sz)

	for i := range ns {
		result = append(result, ns[i]...)
	}

	return result
}
