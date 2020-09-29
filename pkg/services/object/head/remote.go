package headsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/pkg/errors"
)

type remoteHeader struct {
	keyStorage *util.KeyStorage

	node *network.Address
}

func (h *remoteHeader) head(ctx context.Context, prm *Prm, handler func(*object.Object)) error {
	key, err := h.keyStorage.GetKey(prm.common.SessionToken())
	if err != nil {
		return errors.Wrapf(err, "(%T) could not receive private key", h)
	}

	addr := h.node.NetAddr()

	c, err := client.New(key,
		client.WithAddress(addr),
	)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not create SDK client %s", h, addr)
	}

	p := new(client.ObjectHeaderParams).
		WithAddress(prm.addr)

	if prm.short {
		p = p.WithMainFields()
	}

	hdr, err := c.GetObjectHeader(ctx, p,
		client.WithTTL(1), // FIXME: use constant
		client.WithSession(prm.common.SessionToken()),
	)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not head object in %s", h, addr)
	}

	handler(object.NewFromSDK(hdr))

	return nil
}
