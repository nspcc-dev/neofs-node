package main

import (
	containerGRPC "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	containerTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/container/grpc"
	containerService "github.com/nspcc-dev/neofs-node/pkg/services/container"
	containerMorph "github.com/nspcc-dev/neofs-node/pkg/services/container/morph"
)

func initContainerService(c *cfg) {
	staticClient, err := client.NewStatic(
		c.cfgMorph.client,
		c.cfgContainer.scriptHash,
		c.cfgContainer.fee,
	)
	fatalOnErr(err)

	cnrClient, err := container.New(staticClient)
	fatalOnErr(err)

	wrap, err := wrapper.New(cnrClient)
	fatalOnErr(err)

	c.cfgObject.cnrStorage = wrap // use RPC node as source of containers
	c.cfgObject.cnrClient = wrap

	containerGRPC.RegisterContainerServiceServer(c.cfgGRPC.server,
		containerTransportGRPC.New(
			containerService.NewSignService(
				c.key,
				containerService.NewResponseService(
					containerService.NewExecutionService(
						containerMorph.NewExecutor(cnrClient),
					),
					c.respSvc,
				),
			),
		),
	)
}
