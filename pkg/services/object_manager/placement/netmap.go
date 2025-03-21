package placement

import (
	"fmt"
	"slices"
	"sync"

	"github.com/hashicorp/golang-lru/v2/simplelru"
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
	containerCache simplelru.LRUCache[string, [][]netmapSDK.NodeInfo]
}

// defaultContainerCacheSize is the default size for the container cache.
const defaultContainerCacheSize = 1000

// Builder is an interface of the
// object placement vector builder.
type Builder interface {
	// BuildPlacement returns the list of placement vectors
	// for object according to the placement policy.
	//
	// Must return all container nodes if object identifier
	// is nil.
	BuildPlacement(cid.ID, *oid.ID, netmapSDK.PlacementPolicy) ([][]netmapSDK.NodeInfo, error)
}

func NewNetworkMapSourceBuilder(nmSrc netmap.Source) Builder {
	cache, _ := simplelru.NewLRU[string, [][]netmapSDK.NodeInfo](defaultContainerCacheSize, nil) // no error
	return &netMapBuilder{
		nmSrc:          nmSrc,
		containerCache: cache,
	}
}

func (b *netMapBuilder) BuildPlacement(cnr cid.ID, obj *oid.ID, p netmapSDK.PlacementPolicy) ([][]netmapSDK.NodeInfo, error) {
	nm, err := b.nmSrc.NetMap()
	if err != nil {
		return nil, fmt.Errorf("could not get network map: %w", err)
	}

	binCnr := cnr[:]

	b.mtx.Lock()
	if nm == b.lastNm {
		cn, ok := b.containerCache.Get(string(binCnr))
		b.mtx.Unlock()
		if ok {
			return BuildObjectPlacement(nm, cn, obj)
		}
	} else {
		b.containerCache.Purge()
		b.mtx.Unlock()
	}

	cn, err := nm.ContainerNodes(p, cnr)
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

	on, err := nm.PlacementVectors(cnrNodes, *id)
	if err != nil {
		return nil, fmt.Errorf("could not get placement vectors for object: %w", err)
	}

	return on, nil
}

// FlattenNodes appends each row to the flat list.
func FlattenNodes(ns [][]netmapSDK.NodeInfo) []netmapSDK.NodeInfo {
	return slices.Concat(ns...)
}
