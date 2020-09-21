package putsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/pkg/errors"
)

type localPlacement struct {
	builder placement.Builder

	localAddrSrc network.LocalAddressSource
}

func (p *localPlacement) BuildPlacement(addr *objectSDK.Address, policy *netmap.PlacementPolicy) ([]netmap.Nodes, error) {
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

type localTarget struct {
	storage *localstore.Storage

	obj *object.RawObject

	payload []byte
}

func (t *localTarget) WriteHeader(obj *object.RawObject) error {
	t.obj = obj

	t.payload = make([]byte, 0, obj.GetPayloadSize())

	return nil
}

func (t *localTarget) Write(p []byte) (n int, err error) {
	t.payload = append(t.payload, p...)

	return len(p), nil
}

func (t *localTarget) Close() (*transformer.AccessIdentifiers, error) {
	if err := t.storage.Put(t.obj.Object()); err != nil {
		return nil, errors.Wrapf(err, "(%T) could not put object to local storage", t)
	}

	return new(transformer.AccessIdentifiers).
		WithSelfID(t.obj.GetID()), nil
}
