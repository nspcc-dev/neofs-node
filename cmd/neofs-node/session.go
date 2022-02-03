package main

import (
	sessionGRPC "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	sessionTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/session/grpc"
	sessionSvc "github.com/nspcc-dev/neofs-node/pkg/services/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
)

func initSessionService(c *cfg) {
	c.privateTokenStore = storage.New()
	addNewEpochNotificationHandler(c, func(ev event.Event) {
		c.privateTokenStore.RemoveOld(ev.(netmap.NewEpoch).EpochNumber())
	})

	server := sessionTransportGRPC.New(
		sessionSvc.NewSignService(
			&c.key.PrivateKey,
			sessionSvc.NewResponseService(
				sessionSvc.NewExecutionService(
					c.privateTokenStore,
				),
				c.respSvc,
			),
		),
	)

	for _, srv := range c.cfgGRPC.servers {
		sessionGRPC.RegisterSessionServiceServer(srv, server)
	}
}
