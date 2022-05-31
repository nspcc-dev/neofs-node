package placement

import (
	"crypto/sha256"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type netMapBuilder struct {
	nmSrc netmap.Source
}

type netMapSrc struct {
	netmap.Source

	nm *netmapSDK.Netmap
}

func NewNetworkMapBuilder(nm *netmapSDK.Netmap) Builder {
	return &netMapBuilder{
		nmSrc: &netMapSrc{nm: nm},
	}
}

func NewNetworkMapSourceBuilder(nmSrc netmap.Source) Builder {
	return &netMapBuilder{
		nmSrc: nmSrc,
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

	cn, err := nm.GetContainerNodes(p, binCnr)
	if err != nil {
		return nil, fmt.Errorf("could not get container nodes: %w", err)
	}

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
