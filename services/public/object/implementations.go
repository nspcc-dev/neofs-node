package object

import (
	"context"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"github.com/pkg/errors"
)

type (
	remoteService struct {
		ps peers.Interface
	}
)

// NewRemoteService is a remote service controller's constructor.
func NewRemoteService(ps peers.Interface) RemoteService {
	return &remoteService{
		ps: ps,
	}
}

func (rs remoteService) Remote(ctx context.Context, addr multiaddr.Multiaddr) (object.ServiceClient, error) {
	con, err := rs.ps.GRPCConnection(ctx, addr, false)
	if err != nil {
		return nil, errors.Wrapf(err, "remoteService.Remote failed on GRPCConnection to %s", addr)
	}

	return object.NewServiceClient(con), nil
}
