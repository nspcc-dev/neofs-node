package main

import (
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	objectTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
)

func initObjectService(c *cfg) {
	objectGRPC.RegisterObjectServiceServer(c.cfgGRPC.server,
		objectTransportGRPC.New(
			objectService.NewSignService(
				c.key,
				new(objectSvc),
			),
		),
	)
}
