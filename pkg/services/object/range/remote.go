package rangesvc

import (
	"context"
	"io"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/pkg/errors"
)

type remoteRangeWriter struct {
	ctx context.Context

	keyStorage *util.KeyStorage

	node *network.Address

	token *token.SessionToken

	addr *object.Address

	rng *object.Range
}

func (r *remoteRangeWriter) WriteTo(w io.Writer) (int64, error) {
	key, err := r.keyStorage.GetKey(r.token)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not receive private key", r)
	}

	addr, err := r.node.IPAddrString()
	if err != nil {
		return 0, err
	}

	c, err := client.New(key,
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
		client.WithSession(r.token),
	)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not read object payload range from %s", r, addr)
	}

	n, err := w.Write(chunk)

	return int64(n), err
}
