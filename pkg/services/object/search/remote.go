package searchsvc

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/pkg/errors"
)

type remoteStream struct {
	prm *Prm

	key *ecdsa.PrivateKey

	addr *network.Address
}

func (s *remoteStream) stream(ctx context.Context, ch chan<- []*object.ID) error {
	addr := s.addr.NetAddr()

	c, err := client.New(s.key,
		client.WithAddress(addr),
	)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not create SDK client %s", s, addr)
	}

	// TODO: add writer parameter to SDK client
	id, err := c.SearchObject(ctx, new(client.SearchObjectParams).
		WithContainerID(s.prm.cid).
		WithSearchFilters(s.prm.query.ToSearchFilters()),
		client.WithTTL(1), // FIXME: use constant
	)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not search objects in %s", s, addr)
	}

	ch <- id

	return nil
}
