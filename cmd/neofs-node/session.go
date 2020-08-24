package main

import (
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	sessionGRPC "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	sessionTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/session/grpc"
	sessionSvc "github.com/nspcc-dev/neofs-node/pkg/services/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
)

func initSessionService(c *cfg) {
	c.privateTokenStore = storage.New()

	metaHdr := new(session.ResponseMetaHeader)
	xHdr := new(session.XHeader)
	xHdr.SetKey("test X-Header key")
	xHdr.SetValue("test X-Header value")
	metaHdr.SetXHeaders([]*session.XHeader{xHdr})

	sessionGRPC.RegisterSessionServiceServer(c.cfgGRPC.server,
		sessionTransportGRPC.New(
			sessionSvc.NewSignService(
				c.key,
				sessionSvc.NewExecutionService(
					c.privateTokenStore,
					metaHdr,
				),
			),
		),
	)
}
