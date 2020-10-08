package main

import (
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	netmapGRPC "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
	crypto "github.com/nspcc-dev/neofs-crypto"
	netmapTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/netmap/grpc"
	netmapService "github.com/nspcc-dev/neofs-node/pkg/services/netmap"
	"github.com/pkg/errors"
)

func initNetmapService(c *cfg) {
	peerInfo := new(netmap.NodeInfo)
	peerInfo.SetAddress(c.localAddr.String())
	peerInfo.SetPublicKey(crypto.MarshalPublicKey(&c.key.PublicKey))
	peerInfo.SetAttributes(c.cfgNodeInfo.attributes)

	c.cfgNodeInfo.info = peerInfo

	netmapGRPC.RegisterNetmapServiceServer(c.cfgGRPC.server,
		netmapTransportGRPC.New(
			netmapService.NewSignService(
				c.key,
				netmapService.NewExecutionService(
					c.cfgNodeInfo.info,
					c.apiVersion,
				),
			),
		),
	)
}

func bootstrapNode(c *cfg) {
	err := c.cfgNetmap.wrapper.AddPeer(c.cfgNodeInfo.info)
	fatalOnErr(errors.Wrap(err, "bootstrap error"))
}
