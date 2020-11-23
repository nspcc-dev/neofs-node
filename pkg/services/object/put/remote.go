package putsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/pkg/errors"
)

type remoteTarget struct {
	transformer.ObjectTarget

	ctx context.Context

	keyStorage *util.KeyStorage

	token *token.SessionToken

	bearer *token.BearerToken

	addr *network.Address

	obj *object.Object

	clientCache *cache.ClientCache

	clientOpts []client.Option
}

// RemoteSender represents utility for
// sending an object to a remote host.
type RemoteSender struct {
	keyStorage *util.KeyStorage

	clientCache *cache.ClientCache

	clientOpts []client.Option
}

// RemotePutPrm groups remote put operation parameters.
type RemotePutPrm struct {
	node *network.Address

	obj *object.Object
}

func (t *remoteTarget) WriteHeader(obj *object.RawObject) error {
	t.obj = obj.Object()

	return nil
}

func (t *remoteTarget) Close() (*transformer.AccessIdentifiers, error) {
	key, err := t.keyStorage.GetKey(t.token)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not receive private key", t)
	}

	addr, err := t.addr.IPAddrString()
	if err != nil {
		return nil, err
	}

	c, err := t.clientCache.Get(key, addr, t.clientOpts...)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not create SDK client %s", t, addr)
	}

	id, err := c.PutObject(t.ctx, new(client.PutObjectParams).
		WithObject(
			t.obj.SDK(),
		),
		client.WithTTL(1), // FIXME: use constant
		client.WithBearer(t.bearer),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not put object to %s", t, addr)
	}

	return new(transformer.AccessIdentifiers).
		WithSelfID(id), nil
}

// NewRemoteSender creates, initializes and returns new RemoteSender instance.
func NewRemoteSender(keyStorage *util.KeyStorage, cache *cache.ClientCache, opts ...client.Option) *RemoteSender {
	return &RemoteSender{
		keyStorage:  keyStorage,
		clientCache: cache,
		clientOpts:  opts,
	}
}

// WithNodeAddress sets network address of the remote node.
func (p *RemotePutPrm) WithNodeAddress(v *network.Address) *RemotePutPrm {
	if p != nil {
		p.node = v
	}

	return p
}

// WithObject sets transferred object.
func (p *RemotePutPrm) WithObject(v *object.Object) *RemotePutPrm {
	if p != nil {
		p.obj = v
	}

	return p
}

// PutObject sends object to remote node.
func (s *RemoteSender) PutObject(ctx context.Context, p *RemotePutPrm) error {
	t := &remoteTarget{
		ctx:         ctx,
		keyStorage:  s.keyStorage,
		addr:        p.node,
		clientCache: s.clientCache,
		clientOpts:  s.clientOpts,
	}

	if err := t.WriteHeader(object.NewRawFromObject(p.obj)); err != nil {
		return errors.Wrapf(err, "(%T) could not send object header", s)
	} else if _, err := t.Close(); err != nil {
		return errors.Wrapf(err, "(%T) could not send object", s)
	}

	return nil
}
