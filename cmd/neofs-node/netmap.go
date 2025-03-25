package main

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	netmaprpc "github.com/nspcc-dev/neofs-contract/rpc/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/metrics"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	netmapService "github.com/nspcc-dev/neofs-node/pkg/services/netmap"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	protonetmap "github.com/nspcc-dev/neofs-sdk-go/proto/netmap"
	"go.uber.org/zap"
)

// defaultEpochDuration is a default epoch duration to replace zero from FS chain.
const defaultEpochDuration = 240

// primary solution of local network state dump.
type networkState struct {
	l *zap.Logger

	epoch         atomic.Uint64
	block         atomic.Uint32
	epochDuration atomic.Uint64

	controlNetStatus atomic.Value // control.NetmapStatus

	nodeInfo atomic.Value // *netmapSDK.NodeInfo

	metrics *metrics.NodeMetrics
}

func newNetworkState(l *zap.Logger) *networkState {
	var nmStatus atomic.Value
	nmStatus.Store(control.NetmapStatus_STATUS_UNDEFINED)

	return &networkState{
		l:                l,
		controlNetStatus: nmStatus,
	}
}

func (s *networkState) CurrentEpoch() uint64 {
	return s.epoch.Load()
}

func (s *networkState) CurrentBlock() uint32 {
	return s.block.Load()
}

func (s *networkState) CurrentEpochDuration() uint64 {
	return s.epochDuration.Load()
}

func (s *networkState) setCurrentEpoch(v uint64) {
	s.epoch.Store(v)

	s.metrics.SetEpoch(v)
}

func (s *networkState) updateEpochDuration(v uint64) {
	if v != 0 {
		s.epochDuration.Store(v)
		return
	}

	s.l.Warn("zero epoch duration received, fallback to default value", zap.Uint64("applied default value", defaultEpochDuration))
	s.epochDuration.Store(defaultEpochDuration)
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
	ni.NetworkEndpoints()(f)
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

	initNetmapState(c)

	server := netmapService.New(&c.key.PrivateKey, c)

	for _, srv := range c.cfgGRPC.servers {
		protonetmap.RegisterNetmapServiceServer(srv, server)
	}

	addNewEpochNotificationHandler(c, func(ev event.Event) {
		c.cfgNetmap.state.setCurrentEpoch(ev.(netmapEvent.NewEpoch).EpochNumber())
	})

	addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
		e := ev.(netmapEvent.NewEpoch).EpochNumber()

		var (
			ni      *netmapSDK.NodeInfo
			err     error
			retries uint64
		)

		expBackoff := backoff.NewExponentialBackOff()

		err = backoff.RetryNotify(
			func() error {
				retries++
				ni, err = c.netmapLocalNodeState(e)
				if errors.Is(err, rpcclient.ErrWSConnLost) {
					return backoff.Permanent(err)
				}
				return err
			},
			expBackoff,
			func(err error, d time.Duration) {
				c.log.Info("retrying due to error", zap.Uint64("epoch", e), zap.Error(err), zap.Duration("retry-after", d))
			})
		if err != nil {
			c.log.Error("could not update node state on new epoch after retries",
				zap.Uint64("epoch", e),
				zap.Uint64("retries", retries),
				zap.Error(err),
			)

			return
		}

		c.handleLocalNodeInfoFromNetwork(ni)

		if !c.needBootstrap() || c.cfgNetmap.reBoostrapTurnedOff.Load() { // fixes #470
			return
		}

		err = c.heartbeat()
		if err != nil {
			c.log.Warn("can't send heartbeat tx", zap.Error(err))
		}
	})

	addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
		err := makeNotaryDeposit(c)
		if err != nil {
			c.log.Error("could not make notary deposit",
				zap.Error(err),
			)
		}
	})

	addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
		epoch := ev.(netmapEvent.NewEpoch).EpochNumber()
		l := c.log.With(zap.Uint64("epoch", epoch))
		l.Info("new epoch event, requesting new network map to sync SN connection caches...")

		nm, err := c.shared.netMapSource.GetNetMapByEpoch(epoch)
		if err != nil {
			l.Info("failed to get network map by new epoch from event to sync SN connection cache", zap.Error(err))
			return
		}

		nodes := nm.Nodes()
		local := slices.IndexFunc(nodes, func(node netmapSDK.NodeInfo) bool { return c.IsLocalKey(node.PublicKey()) })
		var wg sync.WaitGroup
		l.Info("syncing SN connection caches with the new network map...")
		for _, c := range []*cache.Clients{c.clientCache, c.putClientCache, c.bgClientCache} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c.SyncWithNewNetmap(nodes, local)
			}()
		}
		wg.Wait()
		l.Info("finished syncing SN connection caches with the new network map")
	})
}

// bootstrapNode adds current node to the Network map.
// Must be called after initNetmapService.
func bootstrapNode(c *cfg) {
	if c.needBootstrap() {
		if c.cfgNetmap.state.controlNetmapStatus() == control.NetmapStatus_OFFLINE {
			c.log.Info("current state is offline")
			err := c.bootstrapOnline()
			fatalOnErrDetails("bootstrap error", err)
		} else {
			c.log.Info("network map contains this node, sending heartbeat")
			err := c.heartbeat()
			if err != nil {
				// Not as critical as the one above, will be
				// updated the next epoch.
				c.log.Warn("heartbeat error", zap.Error(err))
			}
		}
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
	epoch, err := c.nCli.Epoch()
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
	nm, err := c.nCli.GetNetMapByEpoch(epoch)
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
	if !c.needBootstrap() {
		return errRelayBootstrap
	}

	var currentStatus = c.cfgNetmap.state.controlNetmapStatus()

	if currentStatus == st {
		return nil // no-op
	}

	if currentStatus == control.NetmapStatus_OFFLINE {
		if st != control.NetmapStatus_ONLINE {
			return errors.New("can't add non-online node to map")
		}
		return c.bootstrapOnline()
	}

	switch st {
	case control.NetmapStatus_OFFLINE:
		return c.updateNetMapState(nil)
	case control.NetmapStatus_MAINTENANCE:
		return c.setMaintenanceStatus(false)
	case control.NetmapStatus_ONLINE:
		c.stopMaintenance()
		return c.updateNetMapState(netmaprpc.NodeStateOnline)
	default:
		return fmt.Errorf("unsupported status %v", st)
	}
}

func (c *cfg) ForceMaintenance() error {
	return c.setMaintenanceStatus(true)
}

func (c *cfg) setMaintenanceStatus(force bool) error {
	netSettings, err := c.nCli.ReadNetworkConfiguration()
	if err != nil {
		err = fmt.Errorf("read network settings to check maintenance allowance: %w", err)
	} else if !netSettings.MaintenanceModeAllowed {
		err = errors.New("maintenance mode is not allowed by the network")
	}

	if err == nil || force {
		c.startMaintenance()

		if err == nil {
			err = c.updateNetMapState(netmaprpc.NodeStateMaintenance)
		}

		if err != nil {
			return fmt.Errorf("local maintenance is started, but state is not updated in the network: %w", err)
		}
	}

	return err
}

// calls UpdatePeerState operation of Netmap contract's client for the local node.
// State setter is used to specify node state to switch to.
func (c *cfg) updateNetMapState(state *big.Int) error {
	return c.nCli.UpdatePeerState(c.key.PublicKey().Bytes(), state)
}

func (c *cfg) GetNetworkInfo() (netmapSDK.NetworkInfo, error) {
	magic, err := c.cfgMorph.client.MagicNumber()
	if err != nil {
		return netmapSDK.NetworkInfo{}, err
	}

	msPerBlock, err := c.cfgMorph.client.MsPerBlock()
	if err != nil {
		return netmapSDK.NetworkInfo{}, fmt.Errorf("ms per block: %w", err)
	}

	netInfoMorph, err := c.nCli.ReadNetworkConfiguration()
	if err != nil {
		return netmapSDK.NetworkInfo{}, fmt.Errorf("read network configuration using netmap contract client: %w", err)
	}

	var ni netmapSDK.NetworkInfo
	ni.SetCurrentEpoch(c.networkState.CurrentEpoch())
	ni.SetMagicNumber(uint64(magic))
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

	return ni, nil
}

func (c *cfg) reloadNodeAttributes() error {
	c.cfgNodeInfo.localInfoLock.Lock()

	oldAttrs := c.cfgNodeInfo.localInfo.GetAttributes()

	c.cfgNodeInfo.localInfo.SetAttributes(nil)

	err := writeSystemAttributes(c)
	if err != nil {
		c.cfgNodeInfo.localInfoLock.Unlock()
		return err
	}
	parseAttributes(c)

	newAttrs := c.cfgNodeInfo.localInfo.GetAttributes()

	c.cfgNodeInfo.localInfoLock.Unlock()

	if nodeAttrsEqual(oldAttrs, newAttrs) {
		return nil
	}

	return c.bootstrapOnline()
}
