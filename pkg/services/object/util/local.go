package util

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/pkg/errors"
)

type localPlacement struct {
	builder placement.Builder

	localAddrSrc network.LocalAddressSource
}

func NewLocalPlacement(b placement.Builder, s network.LocalAddressSource) placement.Builder {
	return &localPlacement{
		builder:      b,
		localAddrSrc: s,
	}
}

func (p *localPlacement) BuildPlacement(addr *object.Address, policy *netmap.PlacementPolicy) ([]netmap.Nodes, error) {
	vs, err := p.builder.BuildPlacement(addr, policy)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not build object placement", p)
	}

	for i := range vs {
		for j := range vs[i] {
			addr, err := network.AddressFromString(vs[i][j].NetworkAddress())
			if err != nil {
				// TODO: log error
				continue
			}

			if network.IsLocalAddress(p.localAddrSrc, addr) {
				return []netmap.Nodes{{vs[i][j]}}, nil
			}
		}
	}

	return nil, errors.Errorf("(%T) local node is outside of object placement", p)
}
