package main

import (
	"bytes"

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

	addNewEpochNotificationHandler(c, func(ev event.Event) {
		e := ev.(netmapEvent.NewEpoch).EpochNumber()

		netStatus, err := c.netmapStatus(e)
		if err != nil {
			c.log.Error("could not update network status on new epoch",
				zap.Uint64("epoch", e),
				zap.String("error", err.Error()),
			)

			return
		}

		c.setNetmapStatus(netStatus)
	})
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

	netStatus, err := c.netmapStatus(epoch)
	fatalOnErr(errors.Wrap(err, "could not init network status"))

	c.setNetmapStatus(netStatus)

	c.log.Info("initial network state",
		zap.Uint64("epoch", epoch),
		zap.Stringer("status", netStatus),
	)

	c.cfgNetmap.state.setCurrentEpoch(epoch)
}

func (c *cfg) netmapStatus(epoch uint64) (control.NetmapStatus, error) {
	// calculate current network state
	nm, err := c.cfgNetmap.wrapper.GetNetMapByEpoch(epoch)
	if err != nil {
		return control.NetmapStatus_STATUS_UNDEFINED, err
	}

	if c.inNetmap(nm) {
		return control.NetmapStatus_ONLINE, nil
	}

	return control.NetmapStatus_OFFLINE, nil
}

func (c *cfg) inNetmap(nm *netmap.Netmap) bool {
	for _, n := range nm.Nodes {
		if bytes.Equal(n.PublicKey(), crypto.MarshalPublicKey(&c.key.PublicKey)) {
			return true
		}
	}

	return false
}

func addNewEpochNotificationHandler(c *cfg, h event.Handler) {
	addNetmapNotificationHandler(c, newEpochNotification, h)
}

func goOffline(c *cfg) {
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

func (c *cfg) SetNetmapStatus(st control.NetmapStatus) error {
	if st == control.NetmapStatus_ONLINE {
		return c.cfgNetmap.wrapper.AddPeer(c.cfgNodeInfo.info)
	}

	var apiState netmap.NodeState

	if st == control.NetmapStatus_OFFLINE {
		apiState = netmap.NodeStateOffline
	}

	return c.cfgNetmap.wrapper.UpdatePeerState(
		crypto.MarshalPublicKey(&c.key.PublicKey),
		apiState,
	)
}
