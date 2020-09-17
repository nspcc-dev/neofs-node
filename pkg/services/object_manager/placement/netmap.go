package placement

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/pkg/errors"
)

type netMapBuilder struct {
	nm *netmap.Netmap
}

func (b *netMapBuilder) BuildPlacement(a *object.Address, p *netmap.PlacementPolicy) ([]netmap.Nodes, error) {
	aV2 := a.ToV2()

	cn, err := b.nm.GetContainerNodes(p, aV2.GetContainerID().GetValue())
	if err != nil {
		return nil, errors.Wrap(err, "could not get container nodes")
	}

	oid := aV2.GetObjectID()
	if oid == nil {
		return cn.Replicas(), nil
	}

	on, err := b.nm.GetPlacementVectors(cn, oid.GetValue())
	if err != nil {
		return nil, errors.Wrap(err, "could not get placement vectors for object")
	}

	return on, nil
}
