package placement

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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

func (b *netMapBuilder) BuildPlacement(a *object.Address, p *netmapSDK.PlacementPolicy) ([]netmapSDK.Nodes, error) {
	nm, err := netmap.GetLatestNetworkMap(b.nmSrc)
	if err != nil {
		return nil, fmt.Errorf("could not get network map: %w", err)
	}

	cn, err := nm.GetContainerNodes(p, a.ContainerID().ToV2().GetValue())
	if err != nil {
		return nil, fmt.Errorf("could not get container nodes: %w", err)
	}

	return BuildObjectPlacement(nm, cn, a.ObjectID())
}

func BuildObjectPlacement(nm *netmapSDK.Netmap, cnrNodes netmapSDK.ContainerNodes, id *object.ID) ([]netmapSDK.Nodes, error) {
	objectID := id.ToV2()
	if objectID == nil {
		return cnrNodes.Replicas(), nil
	}

	on, err := nm.GetPlacementVectors(cnrNodes, objectID.GetValue())
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
