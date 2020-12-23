package placement

import (
	netmapSDK "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/pkg/errors"
)

type netMapBuilder struct {
	nmSrc netmap.Source
}

type netMapSrc struct {
	nm *netmapSDK.Netmap
}

func NewNetworkMapBuilder(nm *netmapSDK.Netmap) Builder {
	return &netMapBuilder{
		nmSrc: &netMapSrc{nm},
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
		return nil, errors.Wrap(err, "could not get network map")
	}

	cn, err := nm.GetContainerNodes(p, a.ContainerID().ToV2().GetValue())
	if err != nil {
		return nil, errors.Wrap(err, "could not get container nodes")
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
		return nil, errors.Wrap(err, "could not get placement vectors for object")
	}

	return on, nil
}
