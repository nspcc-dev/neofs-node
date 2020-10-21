package main

import (
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	netmapGRPC "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
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

func addNetmapNotificationHandler(c *cfg, sTyp string, h event.Handler) {
	typ := event.TypeFromString(sTyp)

	if c.cfgNetmap.subscribers == nil {
		c.cfgNetmap.subscribers = make(map[event.Type][]event.Handler, 1)
	}

	c.cfgNetmap.subscribers[typ] = append(c.cfgNetmap.subscribers[typ], h)
}

func setNetmapNotificationParser(c *cfg, sTyp string, p event.Parser) {
	typ := event.TypeFromString(sTyp)

	if c.cfgNetmap.parsers == nil {
		c.cfgNetmap.parsers = make(map[event.Type]event.Parser, 1)
	}

	c.cfgNetmap.parsers[typ] = p
}

func addNewEpochNotificationHandler(c *cfg, h event.Handler) {
	addNetmapNotificationHandler(c, newEpochNotification, h)
}
