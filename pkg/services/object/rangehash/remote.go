package rangehashsvc

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/pkg/errors"
)

type remoteHasher struct {
	keyStorage *util.KeyStorage

	node *network.Address
}

func (h *remoteHasher) hashRange(ctx context.Context, prm *Prm, handler func([][]byte)) error {
	key, err := h.keyStorage.GetKey(prm.common.SessionToken())
	if err != nil {
		return errors.Wrapf(err, "(%T) could not receive private key", h)
	}

	addr, err := h.node.NetAddr()
	if err != nil {
		return err
	}

	c, err := client.New(key,
		client.WithAddress(addr),
	)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not create SDK client %s", h, addr)
	}

	hashes := make([][]byte, 0, len(prm.rngs))

	p := new(client.RangeChecksumParams).
		WithAddress(prm.addr).
		WithSalt(prm.salt).
		WithRangeList(prm.rngs...)

	opts := []client.CallOption{
		client.WithTTL(1), // FIXME: use constant
		client.WithSession(prm.common.SessionToken()),
	}

	switch prm.typ {
	default:
		panic(fmt.Sprintf("unexpected checksum type %v", prm.typ))
	case pkg.ChecksumSHA256:
		v, err := c.ObjectPayloadRangeSHA256(ctx, p, opts...)
		if err != nil {
			return errors.Wrapf(err, "(%T) could not get SHA256 checksum from %s", h, addr)
		}

		for i := range v {
			hashes = append(hashes, v[i][:])
		}
	case pkg.ChecksumTZ:
		v, err := c.ObjectPayloadRangeTZ(ctx, p, opts...)
		if err != nil {
			return errors.Wrapf(err, "(%T) could not get Tillich-Zemor checksum from %s", h, addr)
		}

		for i := range v {
			hashes = append(hashes, v[i][:])
		}
	}

	handler(hashes)

	return nil
}
