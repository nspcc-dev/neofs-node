package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	neogoutil "github.com/nspcc-dev/neo-go/pkg/util"
	netmaprpc "github.com/nspcc-dev/neofs-contract/rpc/netmap"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/internal/chaintime"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/metrics"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	balanceClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"github.com/nspcc-dev/neofs-node/pkg/services/meta"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/policer"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/sidechain"
	"github.com/nspcc-dev/neofs-node/pkg/timers"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/term"
	"google.golang.org/grpc"
)

const maxMsgSize = 4 << 20 // transport msg limit 4 MiB

// capacity of the pools of the morph notification handlers
// for each contract listener.
const notificationHandlerPoolSize = 10

const (
	metricName   = "prometheus"
	profilerName = "pprof"
)

// errIncorrectStatus is returned from heartbeat when it can't be performed
// because of incorrect (likely offline) status.
var errIncorrectStatus = errors.New("incorrect current network map status")

// internals contains application-specific internals that are created
// on application startup and are shared b/w the components during
// the application life cycle.
// It should not contain any read configuration values, component-specific
// helpers and fields.
type internals struct {
	ctx         context.Context
	ctxCancel   func()
	internalErr chan error // channel for internal application errors at runtime

	logLevel zap.AtomicLevel
	log      *zap.Logger

	wg      *sync.WaitGroup
	workers []worker
	closers []func()
	// services that are useful for debug (e.g. when a regular closer does not
	// close), must be close at the very end of application life cycle
	veryLastClosersLock sync.RWMutex
	veryLastClosers     map[string]func()

	apiVersion   version.Version
	healthStatus atomic.Int32
	// is node under maintenance
	isMaintenance atomic.Bool
}

// starts node's maintenance.
func (c *cfg) startMaintenance() {
	c.isMaintenance.Store(true)
	c.cfgNetmap.state.setControlNetmapStatus(control.NetmapStatus_MAINTENANCE)
	c.log.Info("started local node's maintenance")
}

// stops node's maintenance.
func (c *internals) stopMaintenance() {
	c.isMaintenance.Store(false)
	c.log.Info("stopped local node's maintenance")
}

type basics struct {
	networkState *networkState

	key          *keys.PrivateKey
	binPublicKey []byte

	cli  *client.Client
	nCli *nmClient.Client
	cCli *cntClient.Client
	bCli *balanceClient.Client

	// all caches are non-nil iff caching is enabled in config
	containerCache     *ttlContainerStorage
	eaclCache          *ttlEACLStorage
	containerListCache *ttlContainerLister
	netmapCache        *lruNetmapSource

	balanceSH    neogoutil.Uint160
	containerSH  neogoutil.Uint160
	netmapSH     neogoutil.Uint160
	reputationSH neogoutil.Uint160
	proxySH      neogoutil.Uint160

	// either cached or not
	cnrSrc container.Source
	cnrLst interface {
		List(*user.ID) ([]cid.ID, error)
	}
	eaclSrc      container.EACLSource
	netMapSource netmapCore.Source
}

// shared contains component-specific structs/helpers that should
// be shared during initialization of the application.
type shared struct {
	// shared b/w logical components but does not
	// depend on them, should be inited first
	basics

	privateTokenStore sessionStorage
	persistate        *state.PersistentStorage

	clientCache    *cache.Clients
	bgClientCache  *cache.Clients
	putClientCache *cache.Clients
	localAddr      network.AddressGroup

	ownerIDFromKey user.ID // user ID calculated from key

	// current network map
	netMap atomic.Value // type netmap.NetMap

	// whether the local node is in the netMap
	localNodeInNetmap atomic.Bool

	policer *policer.Policer

	replicator *replicator.Replicator

	metricsCollector *metrics.NodeMetrics

	control *controlSvc.Server

	metaService *meta.Meta
	sidechain   *sidechain.SideChain

	containerPayments *paymentChecker
}

func (s *shared) resetCaches() {
	if s.containerCache != nil {
		s.containerCache.reset()
	}
	if s.eaclCache != nil {
		s.eaclCache.reset()
	}
	if s.containerListCache != nil {
		s.containerListCache.reset()
	}
	if s.netmapCache != nil {
		s.netmapCache.reset()
	}
}

type cfg struct {
	internals
	shared

	appCfg *config.Config

	// configuration of the internal
	// services
	cfgGRPC           cfgGRPC
	cfgMeta           cfgMeta
	cfgMorph          cfgMorph
	cfgBalance        cfgBalance
	cfgContainer      cfgContainer
	cfgNodeInfo       cfgNodeInfo
	cfgNetmap         cfgNetmap
	cfgControlService cfgControlService
	cfgReputation     cfgReputation
	cfgObject         cfgObject

	// chainTime is a global chain time provider updated from FS headers.
	chainTime chaintime.AtomicChainTimeProvider
}

// GetNetworkMap reads network map which has been cached at the latest epoch.
// Returns an error if value has not been cached yet.
//
// Provides interface for NetmapService server.
func (c *cfg) GetNetworkMap() (netmap.NetMap, error) {
	val := c.netMap.Load()
	if val == nil {
		return netmap.NetMap{}, errors.New("missing local network map")
	}

	return val.(netmap.NetMap), nil
}

// CurrentEpoch returns the latest cached epoch.
func (c *cfg) CurrentEpoch() uint64 { return c.networkState.CurrentEpoch() }

type cfgGRPC struct {
	listeners []net.Listener

	servers []*grpc.Server
}

type cfgMeta struct {
	network meta.NeoFSNetwork
}

type cfgMorph struct {
	client *client.Client

	epochTimers      *timers.EpochTimers
	eigenTrustTicker *eigenTrustTickers // timers for EigenTrust iterations

	proxyScriptHash neogoutil.Uint160
}

type cfgBalance struct {
	parsers     map[event.Type]event.NotificationParser
	subscribers map[event.Type][]event.Handler
}

type cfgContainer struct {
	parsers     map[event.Type]event.NotificationParser
	subscribers map[event.Type][]event.Handler
	workerPool  util.WorkerPool // pool for asynchronous handlers
}

type cfgNetmap struct {
	parsers map[event.Type]event.NotificationParser

	subscribers map[event.Type][]event.Handler
	workerPool  util.WorkerPool // pool for asynchronous handlers

	state *networkState

	needBootstrap       bool
	reBoostrapTurnedOff atomic.Bool // managed by control service in runtime
	startEpoch          uint64      // epoch number when application is started
}

type cfgNodeInfo struct {
	// values from config; NOT MODIFY AFTER APP INITIALIZATION OR CONFIG RELOAD
	localInfoLock sync.RWMutex
	localInfo     netmap.NodeInfo
}

type cfgObject struct {
	getSvc *getsvc.Service

	poolLock sync.RWMutex
	pool     cfgObjectRoutines

	cfgLocalStorage cfgLocalStorage

	tombstoneLifetime uint64

	quotasTTL      time.Duration
	containerNodes *containerNodes
}

type cfgLocalStorage struct {
	localStorage *engine.StorageEngine
}

type cfgObjectRoutines struct {
	putRemote *ants.Pool

	replication *ants.Pool

	search *ants.Pool
}

type cfgControlService struct {
	server *grpc.Server
}

type cfgReputation struct {
	workerPool util.WorkerPool // pool for EigenTrust algorithm's iterations

	localTrustStorage *truststorage.Storage

	localTrustCtrl *trustcontroller.Controller
}

var (
	persistateFSChainLastBlockKey             = []byte("fs_chain_last_processed_block")
	persistateDeprecatedSidechainLastBlockKey = []byte("side_chain_last_processed_block")
)

func initCfg(appCfg *config.Config) *cfg {
	c := &cfg{}
	ctx, ctxCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	c.appCfg = appCfg

	c.cfgNodeInfo.localInfoLock.Lock()
	// filling system attributes; do not move it anywhere
	// below applying the other attributes since a user
	// should be able to overwrite it.
	err := writeSystemAttributes(c)
	c.cfgNodeInfo.localInfoLock.Unlock()
	fatalOnErr(err)

	key := appCfg.Node.PrivateKey()

	var netAddr network.AddressGroup

	relayOnly := appCfg.Node.Relay
	if !relayOnly {
		netAddr = appCfg.Node.BootstrapAddresses()
	}

	containerWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	netmapWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	reputationWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	c.internals = internals{
		ctx:         ctx,
		ctxCancel:   ctxCancel,
		internalErr: make(chan error, 10), // We only need one error, but we can have multiple senders.
		wg:          new(sync.WaitGroup),
		apiVersion:  version.Current(),
	}
	c.healthStatus.Store(int32(control.HealthStatus_HEALTH_STATUS_UNDEFINED))

	c.logLevel, err = zap.ParseAtomicLevel(c.appCfg.Logger.Level)
	fatalOnErr(err)

	logCfg := zap.NewProductionConfig()
	logCfg.Level = c.logLevel
	logCfg.Encoding = c.appCfg.Logger.Encoding
	if !c.appCfg.Logger.Sampling.Enabled {
		logCfg.Sampling = nil
	}

	if (term.IsTerminal(int(os.Stdout.Fd())) && !c.appCfg.IsSet("logger.timestamp")) || c.appCfg.Logger.Timestamp {
		logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else {
		logCfg.EncoderConfig.EncodeTime = func(_ time.Time, _ zapcore.PrimitiveArrayEncoder) {}
	}

	c.log, err = logCfg.Build(
		zap.AddStacktrace(zap.NewAtomicLevelAt(zap.FatalLevel)),
	)
	fatalOnErr(err)

	var buffers sync.Pool
	buffers.New = func() any {
		b := make([]byte, cache.DefaultBufferSize)
		return &b
	}

	persistate, err := state.NewPersistentStorage(appCfg.Node.PersistentState.Path, true,
		state.WithLogger(c.log),
		state.WithTimeout(time.Second),
		state.WithEncryptionKey(&key.PrivateKey),
	)
	fatalOnErr(err)

	// TODO: drop deprecated 'node.persistent_sessions.path' in future releases
	persistentSessionPath := c.appCfg.Node.PersistentSessions.Path
	if persistentSessionPath != "" {
		c.log.Warn("'node.persistent_sessions.path' is deprecated, now it is located in 'node.persistent_state.path'")
		err = persistate.MigrateOldTokenStorage(persistentSessionPath)
		fatalOnErr(err)
	}

	basicSharedConfig := initBasics(c, key, persistate)
	streamTimeout := appCfg.APIClient.StreamTimeout
	minConnTimeout := appCfg.APIClient.MinConnectionTime
	pingInterval := appCfg.APIClient.PingInterval
	pingTimeout := appCfg.APIClient.PingTimeout
	newClientCache := func(scope string) *cache.Clients {
		return cache.NewClients(c.log.With(zap.String("scope", scope)), &buffers, streamTimeout,
			minConnTimeout, pingInterval, pingTimeout)
	}
	c.shared = shared{
		basics:            basicSharedConfig,
		localAddr:         netAddr,
		clientCache:       newClientCache("read"),
		bgClientCache:     newClientCache("background"),
		putClientCache:    newClientCache("put"),
		persistate:        persistate,
		privateTokenStore: persistate,
	}
	c.cfgBalance = cfgBalance{
		parsers:     make(map[event.Type]event.NotificationParser),
		subscribers: make(map[event.Type][]event.Handler),
	}
	c.cfgContainer = cfgContainer{
		workerPool: containerWorkerPool,
	}
	c.cfgNetmap = cfgNetmap{
		state:         c.networkState,
		workerPool:    netmapWorkerPool,
		needBootstrap: !relayOnly,
	}
	c.cfgMorph = cfgMorph{
		proxyScriptHash: c.proxySH,
	}
	c.cfgObject = cfgObject{
		pool:              initObjectPool(appCfg),
		tombstoneLifetime: appCfg.Object.Delete.TombstoneLifetime,
		quotasTTL:         c.appCfg.FSChain.QuotaCacheTTL,
	}
	c.cfgReputation = cfgReputation{
		workerPool: reputationWorkerPool,
	}

	c.cfgNetmap.reBoostrapTurnedOff.Store(relayOnly)

	c.ownerIDFromKey = user.NewFromECDSAPublicKey(key.PrivateKey.PublicKey)

	c.metricsCollector = metrics.NewNodeMetrics(misc.Version)
	c.networkState.metrics = c.metricsCollector

	c.veryLastClosers = make(map[string]func())

	c.onShutdown(c.clientCache.CloseAll)    // clean up connections
	c.onShutdown(c.bgClientCache.CloseAll)  // clean up connections
	c.onShutdown(c.putClientCache.CloseAll) // clean up connections
	c.onShutdown(func() { _ = c.persistate.Close() })

	initPaymentChecker(c)

	return c
}

func initBasics(c *cfg, key *keys.PrivateKey, stateStorage *state.PersistentStorage) basics {
	b := basics{}

	addresses := c.appCfg.FSChain.Endpoints

	fromDeprectedSidechanBlock, err := stateStorage.UInt32(persistateDeprecatedSidechainLastBlockKey)
	if err != nil {
		fromDeprectedSidechanBlock = 0
	}
	fromFSChainBlock, err := stateStorage.UInt32(persistateFSChainLastBlockKey)
	if err != nil {
		fromFSChainBlock = 0
		c.log.Warn("can't get last processed FS chain block number", zap.Error(err))
	}

	// migration for deprecated DB key
	if fromFSChainBlock == 0 && fromDeprectedSidechanBlock != fromFSChainBlock {
		fromFSChainBlock = fromDeprectedSidechanBlock
		err = stateStorage.SetUInt32(persistateFSChainLastBlockKey, fromFSChainBlock)
		if err != nil {
			c.log.Warn("can't update persistent state",
				zap.String("chain", "FS"),
				zap.Uint32("block_index", fromFSChainBlock))
		}

		err = stateStorage.Delete(persistateDeprecatedSidechainLastBlockKey)
		if err != nil {
			c.log.Warn("can't delete deprecated persistent state", zap.Error(err))
		}
	}

	cli, err := client.New(key,
		client.WithContext(c.ctx),
		client.WithDialTimeout(c.appCfg.FSChain.DialTimeout),
		client.WithLogger(c.log),
		client.WithAutoFSChainScope(),
		client.WithEndpoints(addresses),
		client.WithReconnectionRetries(c.appCfg.FSChain.ReconnectionsNumber),
		client.WithReconnectionsDelay(c.appCfg.FSChain.ReconnectionsDelay),
		client.WithConnSwitchCallback(func() {
			err = c.restartMorph()
			if err != nil {
				c.internalErr <- fmt.Errorf("restarting after morph connection was lost: %w", err)
			}
		}),
		client.WithMinRequiredBlockHeight(fromFSChainBlock),
	)
	if err != nil {
		c.log.Info("failed to create neo RPC client",
			zap.Any("endpoints", addresses),
			zap.Error(err),
		)

		fatalOnErr(err)
	}

	lookupScriptHashesInNNS(cli, &b)

	nState := newNetworkState(c.log)
	currBlock, err := cli.BlockCount()
	fatalOnErr(err)
	nState.block.Store(currBlock)

	b.bCli, err = balanceClient.NewFromMorph(cli, b.balanceSH)
	fatalOnErr(err)

	b.cCli, err = cntClient.NewFromMorph(cli, b.containerSH)
	fatalOnErr(err)

	b.nCli, err = nmClient.NewFromMorph(cli, b.netmapSH)
	fatalOnErr(err)

	eDuration, err := b.nCli.EpochDuration()
	fatalOnErr(err)
	nState.updateEpochDuration(eDuration)

	ttl := c.appCfg.FSChain.CacheTTL

	b.netMapSource = b.nCli
	b.cnrSrc = cntClient.AsContainerSource(b.cCli)
	b.eaclSrc = b.cCli
	b.cnrLst = b.cCli
	if ttl >= 0 {
		b.netmapCache = newCachedNetmapStorage(nState, b.netMapSource)
		b.netMapSource = b.netmapCache
		b.containerCache = newCachedContainerStorage(b.cnrSrc, ttl)
		b.cnrSrc = b.containerCache
		b.eaclCache = newCachedEACLStorage(b.eaclSrc, ttl)
		b.eaclSrc = b.eaclCache
		b.containerListCache = newCachedContainerLister(b.cCli, ttl)
		b.cnrLst = b.containerListCache
	}

	b.networkState = nState
	b.key = key
	b.binPublicKey = key.PublicKey().Bytes()
	b.cli = cli

	return b
}

func (c *cfg) policerOpts() []policer.Option {
	pCfg := c.appCfg.Policer

	return []policer.Option{
		policer.WithMaxCapacity(pCfg.MaxWorkers),
		policer.WithHeadTimeout(pCfg.HeadTimeout),
		policer.WithReplicationCooldown(pCfg.ReplicationCooldown),
		policer.WithObjectBatchSize(pCfg.ObjectBatchSize),
	}
}

func initObjectPool(cfg *config.Config) (pool cfgObjectRoutines) {
	var err error

	optNonBlocking := ants.WithNonblocking(true)

	pool.putRemote, err = ants.NewPool(cfg.Object.Put.PoolSizeRemote, optNonBlocking)
	fatalOnErr(err)

	replicatorPoolSize := cfg.Replicator.PoolSize
	if replicatorPoolSize <= 0 {
		replicatorPoolSize = cfg.Object.Put.PoolSizeRemote
	}

	pool.replication, err = ants.NewPool(replicatorPoolSize)
	fatalOnErr(err)

	pool.search, err = ants.NewPool(cfg.Object.Search.PoolSize, optNonBlocking)
	fatalOnErr(err)

	return pool
}

func (c *cfg) reloadObjectPoolSizes() {
	c.cfgObject.poolLock.Lock()
	defer c.cfgObject.poolLock.Unlock()

	c.cfgObject.pool.putRemote.Tune(c.appCfg.Object.Put.PoolSizeRemote)

	replicatorPoolSize := c.appCfg.Replicator.PoolSize
	if replicatorPoolSize <= 0 {
		replicatorPoolSize = c.appCfg.Object.Put.PoolSizeRemote
	}
	c.cfgObject.pool.replication.Tune(replicatorPoolSize)

	c.cfgObject.pool.search.Tune(c.appCfg.Object.Search.PoolSize)
}

func (c *cfg) LocalNodeInfo() (netmap.NodeInfo, error) {
	c.cfgNodeInfo.localInfoLock.RLock()
	defer c.cfgNodeInfo.localInfoLock.RUnlock()

	return c.cfgNodeInfo.localInfo, nil
}

// handleLocalNodeInfoFromNetwork rewrites cached node info from the NeoFS network map.
// Called with nil when storage node is outside the NeoFS network map
// (before entering the network and after leaving it).
func (c *cfg) handleLocalNodeInfoFromNetwork(ni *netmap.NodeInfo) {
	c.cfgNetmap.state.setNodeInfo(ni)
	c.localNodeInNetmap.Store(ni != nil)
}

// bootstrapOnline puts node into the map with online state.
func (c *cfg) bootstrapOnline() error {
	c.log.Info("bootstrapping with online state")
	c.cfgNodeInfo.localInfoLock.RLock()
	ni := c.cfgNodeInfo.localInfo
	c.cfgNodeInfo.localInfoLock.RUnlock()
	ni.SetOnline()

	return c.nCli.AddPeer(ni, c.key.PublicKey())
}

// heartbeat sends UpdatePeer transaction with the current
// node state which can be "maintenance" or "online".
func (c *cfg) heartbeat() error {
	var (
		currentStatus = c.cfgNetmap.state.controlNetmapStatus()
		st            *big.Int
	)
	switch currentStatus {
	case control.NetmapStatus_MAINTENANCE:
		st = netmaprpc.NodeStateMaintenance
	case control.NetmapStatus_ONLINE:
		st = netmaprpc.NodeStateOnline
	default:
		return fmt.Errorf("%w: %v", errIncorrectStatus, currentStatus)
	}
	return c.updateNetMapState(st)
}

// needBootstrap checks if local node should be registered in network on bootup.
func (c *cfg) needBootstrap() bool {
	return c.cfgNetmap.needBootstrap
}

// ObjectServiceLoad implements system loader interface for policer component.
// It is calculated as size/capacity ratio of "remote object put" worker.
// Returns float value between 0.0 and 1.0.
func (c *cfg) ObjectServiceLoad() float64 {
	return float64(c.cfgObject.pool.putRemote.Running()) / float64(c.cfgObject.pool.putRemote.Cap())
}

func (c *cfg) configWatcher(ctx context.Context) {
	var err error
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)

	for {
		select {
		case <-ch:
			c.log.Info("SIGHUP has been received, rereading configuration...")

			oldMetrics := writeMetricConfig(c.appCfg)
			oldProfiler := writeProfilerConfig(c.appCfg)

			c.appCfg, err = config.New(config.WithConfigFile(c.appCfg.Path()))
			if err != nil {
				c.log.Error("configuration reading", zap.Error(err))
				continue
			}

			// Pool

			c.reloadObjectPoolSizes()

			// Prometheus and pprof

			// nolint:contextcheck
			c.reloadMetricsAndPprof(oldMetrics, oldProfiler)

			// Logger

			err = c.logLevel.UnmarshalText([]byte(c.appCfg.Logger.Level))
			if err != nil {
				c.log.Error("invalid logger level configuration", zap.Error(err))
				continue
			}

			// Policer

			c.policer.Reload(c.policerOpts()...)

			// Storage Engine

			var rcfg engine.ReConfiguration
			for _, optsWithID := range c.shardOpts() {
				rcfg.AddShard(optsWithID.configID, optsWithID.shOpts)
			}
			rcfg.SetShardPoolSize(uint32(c.appCfg.Storage.ShardPoolSize))

			err = c.cfgObject.cfgLocalStorage.localStorage.Reload(rcfg)
			if err != nil {
				c.log.Error("storage engine configuration update", zap.Error(err))
				continue
			}

			// Morph

			c.cli.Reload(client.WithEndpoints(c.appCfg.FSChain.Endpoints))

			// Node

			err = c.reloadNodeAttributes()
			if err != nil {
				c.log.Error("invalid node attributes configuration", zap.Error(err))
				continue
			}

			// Meta service

			var p meta.Parameters
			p.NeoEnpoints = c.appCfg.FSChain.Endpoints
			err = c.metaService.Reload(p)
			if err != nil {
				c.log.Error("failed to reload meta service configuration", zap.Error(err))
				continue
			}

			c.log.Info("configuration has been reloaded successfully")
		case <-ctx.Done():
			return
		}
	}
}

// writeSystemAttributes writes app version as defined at compilation
// step to the node's attributes and an aggregated disk capacity
// according to the all space on the all configured disks.
func writeSystemAttributes(c *cfg) error {
	// `Version` attribute

	c.cfgNodeInfo.localInfo.SetVersion(misc.Version)

	// `Capacity` attribute

	var paths []string
	for _, sh := range c.appCfg.Storage.ShardList {
		path, err := getInitPath(sh.Blobstor.Path)
		if err != nil {
			return err
		}

		paths = append(paths, path)
	}

	total, err := totalBytes(paths)
	if err != nil {
		return fmt.Errorf("calculating capacity on every shard: %w", err)
	}

	c.cfgNodeInfo.localInfo.SetCapacity(total / (1 << 30))

	return nil
}

func getInitPath(path string) (string, error) {
	for len(path) > 1 { // Dir returns / or . if nothing else left.
		fi, err := os.Stat(path)
		if err == nil && fi.IsDir() {
			break
		}
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("accessing %q: %w", path, err)
		}
		path = filepath.Dir(path)
	}

	return path, nil
}

func (c *cfg) reloadMetricsAndPprof(oldMetrics metricConfig, oldProfiler profilerConfig) {
	c.veryLastClosersLock.Lock()
	defer c.veryLastClosersLock.Unlock()

	// Metrics

	if oldMetrics.isUpdated(c.appCfg) {
		if closer, ok := c.veryLastClosers[metricName]; ok {
			closer()
		}
		delete(c.veryLastClosers, metricName)

		preRunAndLog(c, metricName, initMetrics(c))
	}

	// Profiler

	if oldProfiler.isUpdated(c.appCfg) {
		if closer, ok := c.veryLastClosers[profilerName]; ok {
			closer()
		}
		delete(c.veryLastClosers, profilerName)

		preRunAndLog(c, profilerName, initProfiler(c))
	}

	tuneProfiles(c.appCfg.Pprof)
}
