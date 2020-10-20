package putsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
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

	c, err := client.New(key,
		client.WithAddress(addr),
	)
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
