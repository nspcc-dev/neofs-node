package node

import (
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/services/public/session"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type sessionParams struct {
	dig.In

	Logger *zap.Logger

	TokenStore session.TokenStore

	EpochReceiver implementations.EpochReceiver
}

func newSessionService(p sessionParams) (session.Service, error) {
	return session.New(session.Params{
		TokenStore:    p.TokenStore,
		Logger:        p.Logger,
		EpochReceiver: p.EpochReceiver,
	}), nil
}
