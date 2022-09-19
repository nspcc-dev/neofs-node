package main

import (
	"bytes"
	"errors"
	"fmt"

	netmapGRPC "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/metrics"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	netmapTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/netmap/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	netmapService "github.com/nspcc-dev/neofs-node/pkg/services/netmap"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// primary solution of local network state dump.
type networkState struct {
	epoch *atomic.Uint64

	controlNetStatus atomic.Value // control.NetmapStatus

	nodeInfo atomic.Value // *netmapSDK.NodeInfo

	metrics *metrics.NodeMetrics
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
	if s.metrics != nil {
		s.metrics.SetEpoch(v)
	}
}

func (s *networkState) setNodeInfo(ni *netmapSDK.NodeInfo) {
	ctrlNetSt := control.NetmapStatus_STATUS_UNDEFINED

	if ni != nil {
		s.nodeInfo.Store(*ni)

		switch {
		case ni.IsOnline():
			ctrlNetSt = control.NetmapStatus_ONLINE
		case ni.IsOffline():
			ctrlNetSt = control.NetmapStatus_OFFLINE
		}
	} else {
		ctrlNetSt = control.NetmapStatus_OFFLINE
	}

	s.controlNetStatus.Store(ctrlNetSt)
}

func (s *networkState) controlNetmapStatus() control.NetmapStatus {
	return s.controlNetStatus.Load().(control.NetmapStatus)
}

func (s *networkState) getNodeInfo() (res netmapSDK.NodeInfo, ok bool) {
	v := s.nodeInfo.Load()
	if v != nil {
		res, ok = v.(netmapSDK.NodeInfo)
		if !ok {
			panic(fmt.Sprintf("unexpected value in atomic node info state: %T", v))
		}
	}

	return
}

func nodeKeyFromNetmap(c *cfg) []byte {
	ni, ok := c.cfgNetmap.state.getNodeInfo()
	if ok {
		return ni.PublicKey()
	}

	return nil
}

func (c *cfg) iterateNetworkAddresses(f func(string) bool) {
	ni, ok := c.cfgNetmap.state.getNodeInfo()
	if ok {
		ni.IterateNetworkEndpoints(f)
	}
}

func (c *cfg) addressNum() int {
	ni, ok := c.cfgNetmap.state.getNodeInfo()
	if ok {
		return ni.NumberOfNetworkEndpoints()
	}

	return 0
}

func initNetmapService(c *cfg) {
	network.WriteToNodeInfo(c.localAddr, &c.cfgNodeInfo.localInfo)
	c.cfgNodeInfo.localInfo.SetPublicKey(c.key.PublicKey().Bytes())
	parseAttributes(c)
	c.cfgNodeInfo.localInfo.SetOffline()

	readSubnetCfg(c)

	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	initNetmapState(c)

	server := netmapTransportGRPC.New(
		netmapService.NewSignService(
			&c.key.PrivateKey,
			netmapService.NewResponseService(
				netmapService.NewExecutionService(
					c,
					c.apiVersion,
					&netInfo{
						netState:          c.cfgNetmap.state,
						magic:             c.cfgMorph.client,
						morphClientNetMap: c.cfgNetmap.wrapper,
						msPerBlockRdr:     c.cfgMorph.client.MsPerBlock,
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

	if c.cfgMorph.notaryEnabled {
		addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
			_, err := makeNotaryDeposit(c)
			if err != nil {
				c.log.Error("could not make notary deposit",
					zap.String("error", err.Error()),
				)
			}
		})
	}
}

func readSubnetCfg(c *cfg) {
	var subnetCfg nodeconfig.SubnetConfig

	subnetCfg.Init(*c.appCfg)

	var (
		id  subnetid.ID
		err error
	)

	subnetCfg.IterateSubnets(func(idTxt string) {
		err = id.DecodeString(idTxt)
		fatalOnErrDetails("parse subnet entry", err)

		c.cfgNodeInfo.localInfo.EnterSubnet(id)
	})

	if subnetCfg.ExitZero() {
		subnetid.MakeZero(&id)
		c.cfgNodeInfo.localInfo.ExitSubnet(id)
	}
}

// bootstrapNode adds current node to the Network map.
// Must be called after initNetmapService.
func bootstrapNode(c *cfg) {
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

func setNetmapNotificationParser(c *cfg, sTyp string, p event.NotificationParser) {
	typ := event.TypeFromString(sTyp)

	if c.cfgNetmap.parsers == nil {
		c.cfgNetmap.parsers = make(map[event.Type]event.NotificationParser, 1)
	}

	c.cfgNetmap.parsers[typ] = p
}

// initNetmapState inits current Network map state.
// Must be called after Morph components initialization.
func initNetmapState(c *cfg) {
	epoch, err := c.cfgNetmap.wrapper.Epoch()
	fatalOnErrDetails("could not initialize current epoch number", err)

	ni, err := c.netmapLocalNodeState(epoch)
	fatalOnErrDetails("could not init network state", err)

	stateWord := "undefined"

	if ni != nil {
		switch {
		case ni.IsOnline():
			stateWord = "online"
		case ni.IsOffline():
			stateWord = "offline"
		}
	}

	c.log.Info("initial network state",
		zap.Uint64("epoch", epoch),
		zap.String("state", stateWord),
	)

	c.cfgNetmap.state.setCurrentEpoch(epoch)
	c.cfgNetmap.startEpoch = epoch
	c.handleLocalNodeInfo(ni)
}

func (c *cfg) netmapLocalNodeState(epoch uint64) (*netmapSDK.NodeInfo, error) {
	// calculate current network state
	nm, err := c.cfgNetmap.wrapper.GetNetMapByEpoch(epoch)
	if err != nil {
		return nil, err
	}

	c.netMap.Store(*nm)

	nmNodes := nm.Nodes()
	for i := range nmNodes {
		if bytes.Equal(nmNodes[i].PublicKey(), c.binPublicKey) {
			return &nmNodes[i], nil
		}
	}

	return nil, nil
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

var errNodeMaintenance = errors.New("node is in maintenance mode")

func (c *cfg) SetNetmapStatus(st control.NetmapStatus) error {
	switch st {
	default:
		return fmt.Errorf("unsupported status %v", st)
	case control.NetmapStatus_MAINTENANCE:
		return c.cfgObject.cfgLocalStorage.localStorage.BlockExecution(errNodeMaintenance)
	case control.NetmapStatus_ONLINE, control.NetmapStatus_OFFLINE:
	}

	err := c.cfgObject.cfgLocalStorage.localStorage.ResumeExecution()
	if err != nil {
		c.log.Error("failed to resume local object operations",
			zap.String("error", err.Error()),
		)
	}

	if !c.needBootstrap() {
		return errRelayBootstrap
	}

	if st == control.NetmapStatus_ONLINE {
		c.cfgNetmap.reBoostrapTurnedOff.Store(false)
		return c.bootstrap()
	}

	c.cfgNetmap.reBoostrapTurnedOff.Store(true)

	prm := nmClient.UpdatePeerPrm{}

	prm.SetKey(c.key.PublicKey().Bytes())

	return c.cfgNetmap.wrapper.UpdatePeerState(prm)
}

type netInfo struct {
	netState netmap.State

	magic interface {
		MagicNumber() (uint64, error)
	}

	morphClientNetMap *nmClient.Client

	msPerBlockRdr func() (int64, error)
}

func (n *netInfo) Dump(ver version.Version) (*netmapSDK.NetworkInfo, error) {
	magic, err := n.magic.MagicNumber()
	if err != nil {
		return nil, err
	}

	var ni netmapSDK.NetworkInfo
	ni.SetCurrentEpoch(n.netState.CurrentEpoch())
	ni.SetMagicNumber(magic)

	netInfoMorph, err := n.morphClientNetMap.ReadNetworkConfiguration()
	if err != nil {
		return nil, fmt.Errorf("read network configuration using netmap contract client: %w", err)
	}

	if mjr := ver.Major(); mjr > 2 || mjr == 2 && ver.Minor() > 9 {
		msPerBlock, err := n.msPerBlockRdr()
		if err != nil {
			return nil, fmt.Errorf("ms per block: %w", err)
		}

		ni.SetMsPerBlock(msPerBlock)

		ni.SetMaxObjectSize(netInfoMorph.MaxObjectSize)
		ni.SetStoragePrice(netInfoMorph.StoragePrice)
		ni.SetAuditFee(netInfoMorph.AuditFee)
		ni.SetEpochDuration(netInfoMorph.EpochDuration)
		ni.SetContainerFee(netInfoMorph.ContainerFee)
		ni.SetNamedContainerFee(netInfoMorph.ContainerAliasFee)
		ni.SetNumberOfEigenTrustIterations(netInfoMorph.EigenTrustIterations)
		ni.SetEigenTrustAlpha(netInfoMorph.EigenTrustAlpha)
		ni.SetIRCandidateFee(netInfoMorph.IRCandidateFee)
		ni.SetWithdrawalFee(netInfoMorph.WithdrawalFee)

		for i := range netInfoMorph.Raw {
			ni.SetRawNetworkParameter(netInfoMorph.Raw[i].Name, netInfoMorph.Raw[i].Value)
		}
	}

	return &ni, nil
}
