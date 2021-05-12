package main

import (
	"bytes"

	netmapSDK "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	netmapV2 "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	netmapGRPC "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
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
	peerInfo := new(netmapSDK.NodeInfo)
	peerInfo.SetAddress(c.localAddr.String())
	peerInfo.SetPublicKey(crypto.MarshalPublicKey(&c.key.PublicKey))
	peerInfo.SetAttributes(c.cfgNodeInfo.attributes...)
	peerInfo.SetState(netmapSDK.NodeStateOffline)

	c.handleLocalNodeInfo(peerInfo)

	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	netmapGRPC.RegisterNetmapServiceServer(c.cfgGRPC.server,
		netmapTransportGRPC.New(
			netmapService.NewSignService(
				c.key,
				netmapService.NewResponseService(
					netmapService.NewExecutionService(
						c,
						c.apiVersion,
						&netInfo{
							netState: c.cfgNetmap.state,
							magic:    c.cfgMorph.client,
						},
					),
					c.respSvc,
				),
			),
		),
	)

	addNewEpochNotificationHandler(c, func(ev event.Event) {
		c.cfgNetmap.state.setCurrentEpoch(ev.(netmapEvent.NewEpoch).EpochNumber())
	})

	addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
		if c.cfgNetmap.reBoostrapTurnedOff.Load() { // fixes #470
			return
		}

		n := ev.(netmapEvent.NewEpoch).EpochNumber()

		const reBootstrapInterval = 2

		if (n-c.cfgNetmap.startEpoch)%reBootstrapInterval == 0 {
			err := c.cfgNetmap.wrapper.AddPeer(c.toOnlineLocalNodeInfo())
			if err != nil {
				c.log.Warn("can't send re-bootstrap tx", zap.Error(err))
			}
		}
	})

	addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
		e := ev.(netmapEvent.NewEpoch).EpochNumber()

		ni, err := c.netmapLocalNodeState(e)
		if err != nil {
			c.log.Error("could not update node state on new epoch",
				zap.Uint64("epoch", e),
				zap.String("error", err.Error()),
			)

			return
		}

		c.handleLocalNodeInfo(ni)
	})
}

func bootstrapNode(c *cfg) {
	initState(c)

	err := c.cfgNetmap.wrapper.AddPeer(c.toOnlineLocalNodeInfo())
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

	ni, err := c.netmapLocalNodeState(epoch)
	fatalOnErr(errors.Wrap(err, "could not init network state"))

	c.handleNodeInfoStatus(ni)

	c.log.Info("initial network state",
		zap.Uint64("epoch", epoch),
		zap.Stringer("state", ni.State()),
	)

	c.cfgNetmap.state.setCurrentEpoch(epoch)
	c.cfgNetmap.startEpoch = epoch
}

func (c *cfg) netmapLocalNodeState(epoch uint64) (*netmapSDK.NodeInfo, error) {
	// calculate current network state
	nm, err := c.cfgNetmap.wrapper.GetNetMapByEpoch(epoch)
	if err != nil {
		return nil, err
	}

	return c.localNodeInfoFromNetmap(nm), nil
}

func (c *cfg) localNodeInfoFromNetmap(nm *netmapSDK.Netmap) *netmapSDK.NodeInfo {
	for _, n := range nm.Nodes {
		if bytes.Equal(n.PublicKey(), crypto.MarshalPublicKey(&c.key.PublicKey)) {
			return n.NodeInfo
		}
	}

	return nil
}

// addNewEpochNotificationHandler adds handler that will be executed synchronously
func addNewEpochNotificationHandler(c *cfg, h event.Handler) {
	addNetmapNotificationHandler(c, newEpochNotification, h)
}

// addNewEpochAsyncNotificationHandler adds handler that will be executed asynchronously via netmap workerPool
func addNewEpochAsyncNotificationHandler(c *cfg, h event.Handler) {
	addNetmapNotificationHandler(
		c,
		newEpochNotification,
		event.WorkerPoolHandler(
			c.cfgNetmap.workerPool,
			h,
			c.log,
		),
	)
}

func (c *cfg) SetNetmapStatus(st control.NetmapStatus) error {
	if st == control.NetmapStatus_ONLINE {
		c.cfgNetmap.reBoostrapTurnedOff.Store(false)
		return c.cfgNetmap.wrapper.AddPeer(c.toOnlineLocalNodeInfo())
	}

	var apiState netmapSDK.NodeState

	if st == control.NetmapStatus_OFFLINE {
		apiState = netmapSDK.NodeStateOffline
	}

	c.cfgNetmap.reBoostrapTurnedOff.Store(true)

	return c.cfgNetmap.wrapper.UpdatePeerState(
		crypto.MarshalPublicKey(&c.key.PublicKey),
		apiState,
	)
}

type netInfo struct {
	netState netmap.State

	magic interface {
		MagicNumber() uint64
	}
}

func (n *netInfo) Dump() (*netmapV2.NetworkInfo, error) {
	ni := new(netmapV2.NetworkInfo)
	ni.SetCurrentEpoch(n.netState.CurrentEpoch())
	ni.SetMagicNumber(n.magic.MagicNumber())

	return ni, nil
}
