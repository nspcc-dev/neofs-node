package putsvc

import (
	"bytes"
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/pkg/errors"
)

type remoteTarget struct {
	transformer.ObjectTarget

	ctx context.Context

	key *ecdsa.PrivateKey

	addr *network.Address

	obj *object.Object
}

func (t *remoteTarget) WriteHeader(obj *object.RawObject) error {
	t.obj = obj.Object()

	return nil
}

func (t *remoteTarget) Close() (*transformer.AccessIdentifiers, error) {
	addr := t.addr.NetAddr()

	c, err := client.New(t.key,
		client.WithAddress(addr),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not create SDK client %s", t, addr)
	}

	id, err := c.PutObject(t.ctx, new(client.PutObjectParams).
		WithObject(
			t.obj.SDK(),
		).
		WithPayloadReader(bytes.NewReader(t.obj.GetPayload())),
		client.WithTTL(1), // FIXME: use constant
	)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not put object to %s", t, addr)
	}

	return new(transformer.AccessIdentifiers).
		WithSelfID(id), nil
}
