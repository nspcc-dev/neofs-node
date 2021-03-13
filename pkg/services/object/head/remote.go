package headsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/pkg/errors"
)

// RemoteHeader represents utility for getting
// the object header from a remote host.
type RemoteHeader struct {
	keyStorage *util.KeyStorage

	clientCache *cache.ClientCache

	clientOpts []client.Option
}

// RemoteHeadPrm groups remote header operation parameters.
type RemoteHeadPrm struct {
	commonHeadPrm *Prm

	node *network.Address
}

var ErrNotFound = errors.New("object header not found")

// NewRemoteHeader creates, initializes and returns new RemoteHeader instance.
func NewRemoteHeader(keyStorage *util.KeyStorage, cache *cache.ClientCache, opts ...client.Option) *RemoteHeader {
	return &RemoteHeader{
		keyStorage:  keyStorage,
		clientCache: cache,
		clientOpts:  opts,
	}
}

// WithNodeAddress sets network address of the remote node.
func (p *RemoteHeadPrm) WithNodeAddress(v *network.Address) *RemoteHeadPrm {
	if p != nil {
		p.node = v
	}

	return p
}

// WithObjectAddress sets object address.
func (p *RemoteHeadPrm) WithObjectAddress(v *objectSDK.Address) *RemoteHeadPrm {
	if p != nil {
		p.commonHeadPrm = new(Prm).WithAddress(v)
	}

	return p
}

// Head requests object header from the remote node.
func (h *RemoteHeader) Head(ctx context.Context, prm *RemoteHeadPrm) (*object.Object, error) {
	key, err := h.keyStorage.GetKey(prm.commonHeadPrm.common.SessionToken())
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not receive private key", h)
	}

	addr, err := prm.node.IPAddrString()
	if err != nil {
		return nil, err
	}

	c, err := h.clientCache.Get(addr, h.clientOpts...)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not create SDK client %s", h, addr)
	}

	p := new(client.ObjectHeaderParams).
		WithAddress(prm.commonHeadPrm.addr).
		WithRawFlag(prm.commonHeadPrm.raw)

	if prm.commonHeadPrm.short {
		p = p.WithMainFields()
	}

	hdr, err := c.GetObjectHeader(ctx, p,
		client.WithTTL(1), // FIXME: use constant
		client.WithSession(prm.commonHeadPrm.common.SessionToken()),
		client.WithBearer(prm.commonHeadPrm.common.BearerToken()),
		client.WithKey(key),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not head object in %s", h, addr)
	}

	return object.NewFromSDK(hdr), nil
}
