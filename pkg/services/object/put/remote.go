package putsvc

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
)

type remoteTarget struct {
	transformer.ObjectTarget

	ctx context.Context

	keyStorage *util.KeyStorage

	commonPrm *util.CommonPrm

	addr network.AddressGroup

	obj *object.Object

	clientConstructor ClientConstructor
}

// RemoteSender represents utility for
// sending an object to a remote host.
type RemoteSender struct {
	keyStorage *util.KeyStorage

	clientConstructor ClientConstructor
}

// RemotePutPrm groups remote put operation parameters.
type RemotePutPrm struct {
	node network.AddressGroup

	obj *object.Object
}

func (t *remoteTarget) WriteHeader(obj *object.RawObject) error {
	t.obj = obj.Object()

	return nil
}

func (t *remoteTarget) Close() (*transformer.AccessIdentifiers, error) {
	key, err := t.keyStorage.GetKey(t.commonPrm.SessionToken())
	if err != nil {
		return nil, fmt.Errorf("(%T) could not receive private key: %w", t, err)
	}

	c, err := t.clientConstructor.Get(t.addr)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not create SDK client %s: %w", t, t.addr, err)
	}

	id, err := c.PutObject(t.ctx, new(client.PutObjectParams).
		WithObject(
			t.obj.SDK(),
		),
		append(
			t.commonPrm.RemoteCallOptions(),
			client.WithTTL(1), // FIXME: use constant
			client.WithKey(key),
		)...,
	)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not put object to %s: %w", t, t.addr, err)
	}

	return new(transformer.AccessIdentifiers).
		WithSelfID(id), nil
}

// NewRemoteSender creates, initializes and returns new RemoteSender instance.
func NewRemoteSender(keyStorage *util.KeyStorage, cons ClientConstructor) *RemoteSender {
	return &RemoteSender{
		keyStorage:        keyStorage,
		clientConstructor: cons,
	}
}

// WithNodeAddress sets network address of the remote node.
func (p *RemotePutPrm) WithNodeAddress(v network.AddressGroup) *RemotePutPrm {
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
		ctx:               ctx,
		keyStorage:        s.keyStorage,
		addr:              p.node,
		clientConstructor: s.clientConstructor,
	}

	if err := t.WriteHeader(object.NewRawFromObject(p.obj)); err != nil {
		return fmt.Errorf("(%T) could not send object header: %w", s, err)
	} else if _, err := t.Close(); err != nil {
		return fmt.Errorf("(%T) could not send object: %w", s, err)
	}

	return nil
}
