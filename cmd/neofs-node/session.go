package main

import (
	sessionGRPC "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	sessionTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/session/grpc"
	sessionSvc "github.com/nspcc-dev/neofs-node/pkg/services/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
)

func initSessionService(c *cfg) {
	c.privateTokenStore = storage.New()

	sessionGRPC.RegisterSessionServiceServer(c.cfgGRPC.server,
		sessionTransportGRPC.New(
			sessionSvc.NewSignService(
				&c.key.PrivateKey,
				sessionSvc.NewResponseService(
					sessionSvc.NewExecutionService(
						c.privateTokenStore,
					),
					c.respSvc,
				),
			),
		),
	)
}
