package main

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	netmapGRPC "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	netmapTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/netmap/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	netmapService "github.com/nspcc-dev/neofs-node/pkg/services/netmap"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// primary solution of local network state dump.
type networkState struct {
	epoch *atomic.Uint64
}

func newNetworkState() *networkState {
	return &networkState{
		epoch: atomic.NewUint64(0),
	}
}

func (s *networkState) CurrentEpoch() uint64 {
	return s.epoch.Load()
}

func (s *networkState) setCurrentEpoch(v uint64) {
	s.epoch.Store(v)
}

func initNetmapService(c *cfg) {
	peerInfo := new(netmap.NodeInfo)
	peerInfo.SetAddress(c.localAddr.String())
	peerInfo.SetPublicKey(crypto.MarshalPublicKey(&c.key.PublicKey))
	peerInfo.SetAttributes(c.cfgNodeInfo.attributes...)

	c.cfgNodeInfo.info = peerInfo

	netmapGRPC.RegisterNetmapServiceServer(c.cfgGRPC.server,
		netmapTransportGRPC.New(
			netmapService.NewSignService(
				c.key,
				netmapService.NewResponseService(
					netmapService.NewExecutionService(
						c.cfgNodeInfo.info.ToV2(),
						c.apiVersion,
					),
					c.respSvc,
				),
			),
		),
	)

	addNewEpochNotificationHandler(c, func(ev event.Event) {
		c.cfgNetmap.state.setCurrentEpoch(ev.(netmapEvent.NewEpoch).EpochNumber())
	})

	if c.cfgNetmap.reBootstrapEnabled {
		addNewEpochNotificationHandler(c, func(ev event.Event) {
			n := ev.(netmapEvent.NewEpoch).EpochNumber()

			if n%c.cfgNetmap.reBootstrapInterval == 0 {
				err := c.cfgNetmap.wrapper.AddPeer(c.cfgNodeInfo.info)
				if err != nil {
					c.log.Warn("can't send re-bootstrap tx", zap.Error(err))
				}
			}
		})
	}
}

func bootstrapNode(c *cfg) {
	initState(c)

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

func initState(c *cfg) {
	epoch, err := c.cfgNetmap.wrapper.Epoch()
	fatalOnErr(errors.Wrap(err, "could not initialize current epoch number"))

	c.log.Info("initial epoch number",
		zap.Uint64("value", epoch),
	)

	c.cfgNetmap.state.setCurrentEpoch(epoch)
}

func addNewEpochNotificationHandler(c *cfg, h event.Handler) {
	addNetmapNotificationHandler(c, newEpochNotification, h)
}

func goOffline(c *cfg) {
	c.setHealthStatus(control.HealthStatus_OFFLINE)

	err := c.cfgNetmap.wrapper.UpdatePeerState(
		crypto.MarshalPublicKey(&c.key.PublicKey),
		netmap.NodeStateOffline,
	)

	if err != nil {
		c.log.Error("could not go offline",
			zap.String("error", err.Error()),
		)
	} else {
		c.log.Info("request to go offline successfully sent")
	}
}
