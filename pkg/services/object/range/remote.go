package rangesvc

import (
	"context"
	"crypto/ecdsa"
	"io"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/pkg/errors"
)

type remoteRangeWriter struct {
	ctx context.Context

	key *ecdsa.PrivateKey

	node *network.Address

	addr *object.Address

	rng *object.Range
}

func (r *remoteRangeWriter) WriteTo(w io.Writer) (int64, error) {
	addr := r.node.NetAddr()

	c, err := client.New(r.key,
		client.WithAddress(addr),
	)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not create SDK client %s", r, addr)
	}

	// TODO: change ObjectPayloadRangeData to implement WriterTo
	chunk, err := c.ObjectPayloadRangeData(r.ctx, new(client.RangeDataParams).
		WithRange(r.rng).
		WithAddress(r.addr),
		client.WithTTL(1), // FIXME: use constant
	)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not read object payload range from %s", r, addr)
	}

	n, err := w.Write(chunk)

	return int64(n), err
}
