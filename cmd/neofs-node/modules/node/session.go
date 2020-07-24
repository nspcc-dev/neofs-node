package node

import (
	session "github.com/nspcc-dev/neofs-node/pkg/network/transport/session/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type sessionParams struct {
	dig.In

	Logger *zap.Logger

	TokenStore session.TokenStore

	EpochReceiver *placement.PlacementWrapper
}

func newSessionService(p sessionParams) (session.Service, error) {
	return session.New(session.Params{
		TokenStore:    p.TokenStore,
		Logger:        p.Logger,
		EpochReceiver: p.EpochReceiver,
	}), nil
}
