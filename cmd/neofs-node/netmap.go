package main

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"

	netmapV2 "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	netmapGRPC "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
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
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"go.uber.org/zap"
)

// primary solution of local network state dump.
type networkState struct {
	epoch atomic.Uint64

	controlNetStatus atomic.Value // control.NetmapStatus

	nodeInfo atomic.Value // *netmapSDK.NodeInfo

	metrics *metrics.NodeMetrics
}

func newNetworkState() *networkState {
	var nmStatus atomic.Value
	nmStatus.Store(control.NetmapStatus_STATUS_UNDEFINED)

	return &networkState{
		controlNetStatus: nmStatus,
	}
}

func (s *networkState) CurrentEpoch() uint64 {
	return s.epoch.Load()
}

func (s *networkState) setCurrentEpoch(v uint64) {
	s.epoch.Store(v)

	s.metrics.SetEpoch(v)
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
		case ni.IsMaintenance():
			ctrlNetSt = control.NetmapStatus_MAINTENANCE
		}
	} else {
		ctrlNetSt = control.NetmapStatus_OFFLINE

		niRaw := s.nodeInfo.Load()
		if niRaw != nil {
			niOld := niRaw.(netmapSDK.NodeInfo)

			// nil ni means that the node is not included
			// in the netmap
			niOld.SetOffline()

			s.nodeInfo.Store(niOld)
		}
	}

	s.setControlNetmapStatus(ctrlNetSt)
}

// sets the current node state to the given value. Subsequent cfg.bootstrap
// calls will process this value to decide what status node should set in the
// network.
func (s *networkState) setControlNetmapStatus(st control.NetmapStatus) {
	s.controlNetStatus.Store(st)
}

func (s *networkState) controlNetmapStatus() (res control.NetmapStatus) {
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
	ni := c.cfgNodeInfo.localInfo
	ni.IterateNetworkEndpoints(f)
}

func (c *cfg) addressNum() int {
	c.cfgNodeInfo.localInfoLock.RLock()
	defer c.cfgNodeInfo.localInfoLock.RUnlock()

	return c.cfgNodeInfo.localInfo.NumberOfNetworkEndpoints()
}

func initNetmapService(c *cfg) {
	c.cfgNodeInfo.localInfoLock.Lock()

	network.WriteToNodeInfo(c.localAddr, &c.cfgNodeInfo.localInfo)
	c.cfgNodeInfo.localInfo.SetPublicKey(c.key.PublicKey().Bytes())
	parseAttributes(c)
	c.cfgNodeInfo.localInfo.SetOffline()

	c.cfgNodeInfo.localInfoLock.Unlock()

	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	c.cfgNetmap.wrapper = c.shared.basics.nCli

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

		c.handleLocalNodeInfoFromNetwork(ni)
	})

	addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
		err := makeNotaryDeposit(c)
		if err != nil {
			c.log.Error("could not make notary deposit",
				zap.String("error", err.Error()),
			)
		}
	})
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
	epoch, ni, err := getNetworkState(c)
	fatalOnErrDetails("getting network state", err)

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

	updateLocalState(c, epoch, ni)
}

func getNetworkState(c *cfg) (uint64, *netmapSDK.NodeInfo, error) {
	epoch, err := c.cfgNetmap.wrapper.Epoch()
	if err != nil {
		return 0, nil, fmt.Errorf("could not get current epoch number: %w", err)
	}

	ni, err := c.netmapLocalNodeState(epoch)
	if err != nil {
		return 0, nil, fmt.Errorf("could not init network state: %w", err)
	}

	return epoch, ni, nil
}

func updateLocalState(c *cfg, epoch uint64, ni *netmapSDK.NodeInfo) {
	c.cfgNetmap.state.setCurrentEpoch(epoch)
	c.cfgNetmap.startEpoch = epoch
	c.handleLocalNodeInfoFromNetwork(ni)
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

// addNewEpochNotificationHandler adds handler that will be executed synchronously.
func addNewEpochNotificationHandler(c *cfg, h event.Handler) {
	addNetmapNotificationHandler(c, newEpochNotification, h)
}

// addNewEpochAsyncNotificationHandler adds handler that will be executed asynchronously via netmap workerPool.
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
	switch st {
	default:
		return fmt.Errorf("unsupported status %v", st)
	case control.NetmapStatus_MAINTENANCE:
		return c.setMaintenanceStatus(false)
	case control.NetmapStatus_ONLINE, control.NetmapStatus_OFFLINE:
	}

	c.stopMaintenance()

	if !c.needBootstrap() {
		return errRelayBootstrap
	}

	if st == control.NetmapStatus_ONLINE {
		c.cfgNetmap.reBoostrapTurnedOff.Store(false)
		return bootstrapOnline(c)
	}

	c.cfgNetmap.reBoostrapTurnedOff.Store(true)

	return c.updateNetMapState(func(*nmClient.UpdatePeerPrm) {})
}

func (c *cfg) ForceMaintenance() error {
	return c.setMaintenanceStatus(true)
}

func (c *cfg) setMaintenanceStatus(force bool) error {
	netSettings, err := c.cfgNetmap.wrapper.ReadNetworkConfiguration()
	if err != nil {
		err = fmt.Errorf("read network settings to check maintenance allowance: %w", err)
	} else if !netSettings.MaintenanceModeAllowed {
		err = errors.New("maintenance mode is not allowed by the network")
	}

	if err == nil || force {
		c.startMaintenance()

		if err == nil {
			err = c.updateNetMapState((*nmClient.UpdatePeerPrm).SetMaintenance)
		}

		if err != nil {
			return fmt.Errorf("local maintenance is started, but state is not updated in the network: %w", err)
		}
	}

	return err
}

// calls UpdatePeerState operation of Netmap contract's client for the local node.
// State setter is used to specify node state to switch to.
func (c *cfg) updateNetMapState(stateSetter func(*nmClient.UpdatePeerPrm)) error {
	var prm nmClient.UpdatePeerPrm
	prm.SetKey(c.key.PublicKey().Bytes())
	stateSetter(&prm)

	return c.cfgNetmap.wrapper.UpdatePeerState(prm)
}

type netInfo struct {
	netState netmap.State

	magic interface {
		MagicNumber() (uint32, error)
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
	ni.SetMagicNumber(uint64(magic))

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

		if netInfoMorph.HomomorphicHashingDisabled {
			ni.DisableHomomorphicHashing()
		}

		if netInfoMorph.MaintenanceModeAllowed {
			ni.AllowMaintenanceMode()
		}

		for i := range netInfoMorph.Raw {
			ni.SetRawNetworkParameter(netInfoMorph.Raw[i].Name, netInfoMorph.Raw[i].Value)
		}
	}

	return &ni, nil
}

func (c *cfg) reloadNodeAttributes() error {
	c.cfgNodeInfo.localInfoLock.Lock()

	// TODO(@End-rey): after updating SDK, rewrite with w/o api netmap. See #3005, neofs-sdk-go#635.
	var ni2 netmapV2.NodeInfo
	c.cfgNodeInfo.localInfo.WriteToV2(&ni2)

	oldAttrs := ni2.GetAttributes()

	ni2.SetAttributes(nil)

	err := c.cfgNodeInfo.localInfo.ReadFromV2(ni2)
	if err != nil {
		c.cfgNodeInfo.localInfoLock.Unlock()
		return err
	}

	err = writeSystemAttributes(c)
	if err != nil {
		c.cfgNodeInfo.localInfoLock.Unlock()
		return err
	}
	parseAttributes(c)

	c.cfgNodeInfo.localInfo.WriteToV2(&ni2)

	newAttrs := ni2.GetAttributes()
	c.cfgNodeInfo.localInfoLock.Unlock()

	if nodeAttrsEqual(nodeAttrsToSlice(oldAttrs), nodeAttrsToSlice(newAttrs)) {
		return nil
	}

	return c.bootstrap()
}
