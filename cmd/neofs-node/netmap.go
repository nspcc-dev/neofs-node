package main

import (
	"bytes"
	"errors"

	netmapSDK "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	netmapV2 "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	netmapGRPC "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	netmapTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/netmap/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	netmapService "github.com/nspcc-dev/neofs-node/pkg/services/netmap"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// primary solution of local network state dump.
type networkState struct {
	epoch *atomic.Uint64

	controlNetStatus atomic.Value // control.NetmapStatus

	nodeInfo atomic.Value // *netmapSDK.NodeInfo
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

func (s *networkState) setNodeInfo(ni *netmapSDK.NodeInfo) {
	s.nodeInfo.Store(ni)

	var ctrlNetSt control.NetmapStatus

	switch ni.State() {
	default:
		ctrlNetSt = control.NetmapStatus_STATUS_UNDEFINED
	case netmapSDK.NodeStateOnline:
		ctrlNetSt = control.NetmapStatus_ONLINE
	case netmapSDK.NodeStateOffline:
		ctrlNetSt = control.NetmapStatus_OFFLINE
	}

	s.controlNetStatus.Store(ctrlNetSt)
}

func (s *networkState) controlNetmapStatus() control.NetmapStatus {
	return s.controlNetStatus.Load().(control.NetmapStatus)
}

func (s *networkState) getNodeInfo() *netmapSDK.NodeInfo {
	return s.nodeInfo.Load().(*netmapSDK.NodeInfo)
}

func nodeKeyFromNetmap(c *cfg) []byte {
	return c.cfgNetmap.state.getNodeInfo().PublicKey()
}

func (c *cfg) iterateNetworkAddresses(f func(string) bool) {
	c.cfgNetmap.state.getNodeInfo().IterateAddresses(f)
}

func (c *cfg) addressNum() int {
	return c.cfgNetmap.state.getNodeInfo().NumberOfAddresses()
}

func initNetmapService(c *cfg) {
	network.WriteToNodeInfo(c.localAddr, &c.cfgNodeInfo.localInfo)
	c.cfgNodeInfo.localInfo.SetPublicKey(c.key.PublicKey().Bytes())
	c.cfgNodeInfo.localInfo.SetAttributes(parseAttributes(c.appCfg)...)
	c.cfgNodeInfo.localInfo.SetState(netmapSDK.NodeStateOffline)

	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	server := netmapTransportGRPC.New(
		netmapService.NewSignService(
			&c.key.PrivateKey,
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
	)

	for _, srv := range c.cfgGRPC.servers {
		netmapGRPC.RegisterNetmapServiceServer(srv, server)
	}

	addNewEpochNotificationHandler(c, func(ev event.Event) {
		c.cfgNetmap.state.setCurrentEpoch(ev.(netmapEvent.NewEpoch).EpochNumber())
	})

	addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
		if !c.needBootstrap() || c.cfgNetmap.reBoostrapTurnedOff.Load() { // fixes #470
			return
		}

		n := ev.(netmapEvent.NewEpoch).EpochNumber()

		const reBootstrapInterval = 2

		if (n-c.cfgNetmap.startEpoch)%reBootstrapInterval == 0 {
			err := c.bootstrap()
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

	if c.needBootstrap() {
		err := c.bootstrap()
		fatalOnErrDetails("bootstrap error", err)
	}
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
	fatalOnErrDetails("could not initialize current epoch number", err)

	ni, err := c.netmapLocalNodeState(epoch)
	fatalOnErrDetails("could not init network state", err)

	c.log.Info("initial network state",
		zap.Uint64("epoch", epoch),
		zap.Stringer("state", ni.State()),
	)

	c.cfgNetmap.state.setCurrentEpoch(epoch)
	c.cfgNetmap.startEpoch = epoch
	c.cfgNetmap.state.setNodeInfo(ni)
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
		if bytes.Equal(n.PublicKey(), c.key.PublicKey().Bytes()) {
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

var errRelayBootstrap = errors.New("setting netmap status is forbidden in relay mode")

func (c *cfg) SetNetmapStatus(st control.NetmapStatus) error {
	if !c.needBootstrap() {
		return errRelayBootstrap
	}

	if st == control.NetmapStatus_ONLINE {
		c.cfgNetmap.reBoostrapTurnedOff.Store(false)
		return c.bootstrap()
	}

	var apiState netmapSDK.NodeState

	if st == control.NetmapStatus_OFFLINE {
		apiState = netmapSDK.NodeStateOffline
	}

	c.cfgNetmap.reBoostrapTurnedOff.Store(true)

	return c.cfgNetmap.wrapper.UpdatePeerState(
		c.key.PublicKey().Bytes(),
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
