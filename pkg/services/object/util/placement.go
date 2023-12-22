package util

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type localPlacement struct {
	builder placement.Builder

	netmapKeys netmap.AnnouncedKeys
}

type remotePlacement struct {
	builder placement.Builder

	netmapKeys netmap.AnnouncedKeys
}

// TraverserGenerator represents tool that generates
// container traverser for the particular need.
type TraverserGenerator struct {
	netMapSrc netmap.Source

	cnrSrc container.Source

	netmapKeys netmap.AnnouncedKeys

	customOpts []placement.Option
}

func NewLocalPlacement(b placement.Builder, s netmap.AnnouncedKeys) placement.Builder {
	return &localPlacement{
		builder:    b,
		netmapKeys: s,
	}
}

func (p *localPlacement) BuildPlacement(cnr cid.ID, obj *oid.ID, policy netmapSDK.PlacementPolicy) ([][]netmapSDK.NodeInfo, error) {
	vs, err := p.builder.BuildPlacement(cnr, obj, policy)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not build object placement: %w", p, err)
	}

	for i := range vs {
		for j := range vs[i] {
			var addr network.AddressGroup

			err := addr.FromIterator(network.NodeEndpointsIterator(vs[i][j]))
			if err != nil {
				continue
			}

			if p.netmapKeys.IsLocalKey(vs[i][j].PublicKey()) {
				return [][]netmapSDK.NodeInfo{{vs[i][j]}}, nil
			}
		}
	}

	return nil, fmt.Errorf("(%T) local node is outside of object placement", p)
}

// NewRemotePlacementBuilder creates, initializes and returns placement builder that
// excludes local node from any placement vector.
func NewRemotePlacementBuilder(b placement.Builder, s netmap.AnnouncedKeys) placement.Builder {
	return &remotePlacement{
		builder:    b,
		netmapKeys: s,
	}
}

func (p *remotePlacement) BuildPlacement(cnr cid.ID, obj *oid.ID, policy netmapSDK.PlacementPolicy) ([][]netmapSDK.NodeInfo, error) {
	vs, err := p.builder.BuildPlacement(cnr, obj, policy)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not build object placement: %w", p, err)
	}

	for i := range vs {
		for j := 0; j < len(vs[i]); j++ {
			var addr network.AddressGroup

			err := addr.FromIterator(network.NodeEndpointsIterator(vs[i][j]))
			if err != nil {
				continue
			}

			if p.netmapKeys.IsLocalKey(vs[i][j].PublicKey()) {
				vs[i] = append(vs[i][:j], vs[i][j+1:]...)
				j--
			}
		}
	}

	return vs, nil
}
