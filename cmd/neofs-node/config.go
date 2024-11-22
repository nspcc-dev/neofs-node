package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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
	netmapV2 "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	apiclientconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/apiclient"
	contractsconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/contracts"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	fstreeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/fstree"
	loggerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/logger"
	morphconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/morph"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	objectconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/object"
	policerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/policer"
	replicatorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/replicator"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/storage"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/metrics"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/policer"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/term"
	"google.golang.org/grpc"
)

const addressSize = 72 // 32 bytes object ID, 32 bytes container ID, 8 bytes protobuf encoding

const maxMsgSize = 4 << 20 // transport msg limit 4 MiB

// capacity of the pools of the morph notification handlers
// for each contract listener.
const notificationHandlerPoolSize = 10

const (
	metricName   = "prometheus"
	profilerName = "pprof"
)

// applicationConfiguration reads and stores component-specific configuration
// values. It should not store any application helpers structs (pointers to shared
// structs).
// It must not be used concurrently.
//
// REFACTOR IS NOT FINISHED, see:
//   - https://github.com/nspcc-dev/neofs-node/issues/1770
//   - https://github.com/nspcc-dev/neofs-node/pull/1815.
type applicationConfiguration struct {
	// _read indicated whether a config
	// has already been read
	_read bool

	logger struct {
		level    string
		encoding string
	}

	engine struct {
		errorThreshold         uint32
		shardPoolSize          uint32
		shards                 []storage.ShardCfg
		isIgnoreUninitedShards bool
		objectPutRetryDeadline time.Duration
	}

	policer struct {
		maxCapacity         uint32
		headTimeout         time.Duration
		replicationCooldown time.Duration
		objectBatchSize     uint32
	}

	morph struct {
		endpoints                 []string
		dialTimeout               time.Duration
		cacheTTL                  time.Duration
		reconnectionRetriesNumber int
		reconnectionRetriesDelay  time.Duration
	}

	contracts struct {
		netmap     neogoutil.Uint160
		balance    neogoutil.Uint160
		container  neogoutil.Uint160
		reputation neogoutil.Uint160
		proxy      neogoutil.Uint160
	}
}

// readConfig fills applicationConfiguration with raw configuration values
// not modifying them.
func (a *applicationConfiguration) readConfig(c *config.Config) error {
	if a._read {
		err := c.Reload()
		if err != nil {
			return fmt.Errorf("could not reload configuration: %w", err)
		}

		err = validateConfig(c)
		if err != nil {
			return fmt.Errorf("configuration's validation: %w", err)
		}

		// clear if it is rereading
		*a = applicationConfiguration{}
	}

	a._read = true

	// Logger

	a.logger.level = loggerconfig.Level(c)
	a.logger.encoding = loggerconfig.Encoding(c)

	// Policer

	a.policer.maxCapacity = policerconfig.MaxWorkers(c)
	a.policer.headTimeout = policerconfig.HeadTimeout(c)
	a.policer.replicationCooldown = policerconfig.ReplicationCooldown(c)
	a.policer.objectBatchSize = policerconfig.ObjectBatchSize(c)

	// Storage Engine

	a.engine.errorThreshold = engineconfig.ShardErrorThreshold(c)
	a.engine.shardPoolSize = engineconfig.ShardPoolSize(c)
	a.engine.isIgnoreUninitedShards = engineconfig.IgnoreUninitedShards(c)
	a.engine.objectPutRetryDeadline = engineconfig.ObjectPutRetryDeadline(c)

	// Morph

	a.morph.endpoints = morphconfig.Endpoints(c)
	a.morph.dialTimeout = morphconfig.DialTimeout(c)
	a.morph.cacheTTL = morphconfig.CacheTTL(c)
	a.morph.reconnectionRetriesNumber = morphconfig.ReconnectionRetriesNumber(c)
	a.morph.reconnectionRetriesDelay = morphconfig.ReconnectionRetriesDelay(c)

	// Contracts

	a.contracts.balance = contractsconfig.Balance(c)
	a.contracts.container = contractsconfig.Container(c)
	a.contracts.netmap = contractsconfig.Netmap(c)
	a.contracts.proxy = contractsconfig.Proxy(c)
	a.contracts.reputation = contractsconfig.Reputation(c)

	return engineconfig.IterateShards(c, false, func(sc *shardconfig.Config) error {
		var sh storage.ShardCfg

		sh.ResyncMetabase = sc.ResyncMetabase()
		sh.Mode = sc.Mode()
		sh.Compress = sc.Compress()
		sh.UncompressableContentType = sc.UncompressableContentTypes()
		sh.SmallSizeObjectLimit = sc.SmallSizeLimit()

		// write-cache

		writeCacheCfg := sc.WriteCache()
		if writeCacheCfg.Enabled() {
			wc := &sh.WritecacheCfg

			wc.Enabled = true
			wc.Path = writeCacheCfg.Path()
			wc.MaxBatchSize = writeCacheCfg.BoltDB().MaxBatchSize()
			wc.MaxBatchDelay = writeCacheCfg.BoltDB().MaxBatchDelay()
			wc.MaxObjSize = writeCacheCfg.MaxObjectSize()
			wc.SmallObjectSize = writeCacheCfg.SmallObjectSize()
			wc.FlushWorkerCount = writeCacheCfg.WorkersNumber()
			wc.SizeLimit = writeCacheCfg.SizeLimit()
			wc.NoSync = writeCacheCfg.NoSync()
		}

		// blobstor with substorages

		blobStorCfg := sc.BlobStor()
		storagesCfg := blobStorCfg.Storages()
		metabaseCfg := sc.Metabase()
		gcCfg := sc.GC()

		if config.BoolSafe(c.Sub("tree"), "enabled") {
			piloramaCfg := sc.Pilorama()
			pr := &sh.PiloramaCfg

			pr.Enabled = true
			pr.Path = piloramaCfg.Path()
			pr.Perm = piloramaCfg.Perm()
			pr.NoSync = piloramaCfg.NoSync()
			pr.MaxBatchSize = piloramaCfg.MaxBatchSize()
			pr.MaxBatchDelay = piloramaCfg.MaxBatchDelay()
		}

		ss := make([]storage.SubStorageCfg, 0, len(storagesCfg))
		for i := range storagesCfg {
			var sCfg storage.SubStorageCfg

			sCfg.Typ = storagesCfg[i].Type()
			sCfg.Path = storagesCfg[i].Path()
			sCfg.Perm = storagesCfg[i].Perm()
			sCfg.FlushInterval = storagesCfg[i].FlushInterval()

			switch storagesCfg[i].Type() {
			case fstree.Type:
				sub := fstreeconfig.From((*config.Config)(storagesCfg[i]))
				sCfg.Depth = sub.Depth()
				sCfg.NoSync = sub.NoSync()
				sCfg.CombinedCountLimit = sub.CombinedCountLimit()
				sCfg.CombinedSizeLimit = sub.CombinedSizeLimit()
				sCfg.CombinedSizeThreshold = sub.CombinedSizeThreshold()
			case peapod.Type:
				// No specific configs, but it's a valid storage type.
			default:
				return fmt.Errorf("invalid storage type: %s", storagesCfg[i].Type())
			}

			ss = append(ss, sCfg)
		}

		sh.SubStorages = ss

		// meta

		m := &sh.MetaCfg

		m.Path = metabaseCfg.Path()
		m.Perm = metabaseCfg.BoltDB().Perm()
		m.MaxBatchDelay = metabaseCfg.BoltDB().MaxBatchDelay()
		m.MaxBatchSize = metabaseCfg.BoltDB().MaxBatchSize()

		// GC

		sh.GcCfg.RemoverBatchSize = gcCfg.RemoverBatchSize()
		sh.GcCfg.RemoverSleepInterval = gcCfg.RemoverSleepInterval()

		a.engine.shards = append(a.engine.shards, sh)

		return nil
	})
}

// internals contains application-specific internals that are created
// on application startup and are shared b/w the components during
// the application life cycle.
// It should not contain any read configuration values, component-specific
// helpers and fields.
type internals struct {
	ctx         context.Context
	ctxCancel   func()
	internalErr chan error // channel for internal application errors at runtime

	cfgReader *config.Config

	logLevel zap.AtomicLevel
	log      *zap.Logger

	wg      *sync.WaitGroup
	workers []worker
	closers []func()
	// services that are useful for debug (e.g. when a regular closer does not
	// close), must be close at the very end of application life cycle
	veryLastClosers map[string]func()

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

// IsMaintenance checks if storage node is under maintenance.
//
// Provides util.NodeState to Object service.
func (c *internals) IsMaintenance() bool {
	return c.isMaintenance.Load()
}

type basics struct {
	networkState *networkState
	netMapSource netmapCore.Source

	key          *keys.PrivateKey
	binPublicKey []byte

	cli  *client.Client
	nCli *nmClient.Client
	cCli *cntClient.Client

	ttl time.Duration

	// caches are non-nil only if ttl > 0
	containerCache     *ttlContainerStorage
	eaclCache          *ttlEACLStorage
	containerListCache *ttlContainerLister
	netmapCache        *lruNetmapSource

	balanceSH    neogoutil.Uint160
	containerSH  neogoutil.Uint160
	netmapSH     neogoutil.Uint160
	reputationSH neogoutil.Uint160
	proxySH      neogoutil.Uint160
}

// shared contains component-specific structs/helpers that should
// be shared during initialization of the application.
type shared struct {
	// shared b/w logical components but does not
	// depend on them, should be inited first
	basics

	privateTokenStore sessionStorage
	persistate        *state.PersistentStorage

	clientCache    *cache.ClientCache
	bgClientCache  *cache.ClientCache
	putClientCache *cache.ClientCache
	localAddr      network.AddressGroup

	ownerIDFromKey user.ID // user ID calculated from key

	// current network map
	netMap atomic.Value // type netmap.NetMap

	// whether the local node is in the netMap
	localNodeInNetmap atomic.Bool

	respSvc *response.Service

	policer *policer.Policer

	replicator *replicator.Replicator

	treeService *tree.Service

	metricsCollector *metrics.NodeMetrics

	control *controlSvc.Server
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
	applicationConfiguration
	internals
	shared

	// configuration of the internal
	// services
	cfgGRPC           cfgGRPC
	cfgMorph          cfgMorph
	cfgContainer      cfgContainer
	cfgNodeInfo       cfgNodeInfo
	cfgNetmap         cfgNetmap
	cfgControlService cfgControlService
	cfgReputation     cfgReputation
	cfgObject         cfgObject
}

// ReadCurrentNetMap reads network map which has been cached at the
// latest epoch. Returns an error if value has not been cached yet.
//
// Provides interface for NetmapService server.
func (c *cfg) ReadCurrentNetMap(msg *netmapV2.NetMap) error {
	val := c.netMap.Load()
	if val == nil {
		return errors.New("missing local network map")
	}

	val.(netmap.NetMap).WriteToV2(msg)

	return nil
}

type cfgGRPC struct {
	listeners []net.Listener

	servers []*grpc.Server

	maxChunkSize uint64

	maxAddrAmount uint64
}

type cfgMorph struct {
	client *client.Client

	eigenTrustTicker *eigenTrustTickers // timers for EigenTrust iterations

	proxyScriptHash neogoutil.Uint160
}

type cfgContainer struct {
	parsers     map[event.Type]event.NotificationParser
	subscribers map[event.Type][]event.Handler
	workerPool  util.WorkerPool // pool for asynchronous handlers
}

type cfgNetmap struct {
	wrapper *nmClient.Client

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

	cnrSource container.Source

	eaclSource container.EACLSource

	poolLock sync.RWMutex
	pool     cfgObjectRoutines

	cfgLocalStorage cfgLocalStorage

	tombstoneLifetime uint64

	containerNodes *containerNodes
}

type cfgLocalStorage struct {
	localStorage *engine.StorageEngine
}

type cfgObjectRoutines struct {
	putRemote *ants.Pool
	putLocal  *ants.Pool

	putRemoteCapacity int

	replicatorPoolSize int

	replication *ants.Pool
}

type cfgControlService struct {
	server *grpc.Server
}

type cfgReputation struct {
	workerPool util.WorkerPool // pool for EigenTrust algorithm's iterations

	localTrustStorage *truststorage.Storage

	localTrustCtrl *trustcontroller.Controller
}

var persistateSideChainLastBlockKey = []byte("side_chain_last_processed_block")

func initCfg(appCfg *config.Config) *cfg {
	c := &cfg{}
	ctx, ctxCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	err := c.readConfig(appCfg)
	if err != nil {
		panic(fmt.Errorf("config reading: %w", err))
	}

	c.cfgNodeInfo.localInfoLock.Lock()
	// filling system attributes; do not move it anywhere
	// below applying the other attributes since a user
	// should be able to overwrite it.
	err = writeSystemAttributes(c)
	c.cfgNodeInfo.localInfoLock.Unlock()
	fatalOnErr(err)

	key := nodeconfig.Wallet(appCfg)

	var netAddr network.AddressGroup

	relayOnly := nodeconfig.Relay(appCfg)
	if !relayOnly {
		netAddr = nodeconfig.BootstrapAddresses(appCfg)
	}

	maxChunkSize := uint64(maxMsgSize) * 3 / 4          // 25% to meta, 75% to payload
	maxAddrAmount := uint64(maxChunkSize) / addressSize // each address is about 72 bytes

	persistate, err := state.NewPersistentStorage(nodeconfig.PersistentState(appCfg).Path())
	fatalOnErr(err)

	containerWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	netmapWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	reputationWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	c.internals = internals{
		ctx:         ctx,
		ctxCancel:   ctxCancel,
		cfgReader:   appCfg,
		internalErr: make(chan error, 10), // We only need one error, but we can have multiple senders.
		wg:          new(sync.WaitGroup),
		apiVersion:  version.Current(),
	}
	c.internals.healthStatus.Store(int32(control.HealthStatus_HEALTH_STATUS_UNDEFINED))

	c.internals.logLevel, err = zap.ParseAtomicLevel(c.logger.level)
	fatalOnErr(err)

	logCfg := zap.NewProductionConfig()
	logCfg.Level = c.internals.logLevel
	logCfg.Encoding = c.logger.encoding
	logCfg.Sampling = nil
	if term.IsTerminal(int(os.Stdout.Fd())) {
		logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else {
		logCfg.EncoderConfig.EncodeTime = func(_ time.Time, _ zapcore.PrimitiveArrayEncoder) {}
	}

	c.internals.log, err = logCfg.Build(
		zap.AddStacktrace(zap.NewAtomicLevelAt(zap.FatalLevel)),
	)
	fatalOnErr(err)

	var buffers sync.Pool
	buffers.New = func() any {
		b := make([]byte, cache.DefaultBufferSize)
		return &b
	}

	cacheOpts := cache.ClientCacheOpts{
		DialTimeout:      apiclientconfig.DialTimeout(appCfg),
		StreamTimeout:    apiclientconfig.StreamTimeout(appCfg),
		AllowExternal:    apiclientconfig.AllowExternal(appCfg),
		ReconnectTimeout: apiclientconfig.ReconnectTimeout(appCfg),
		Buffers:          &buffers,
		Logger:           c.internals.log,
	}
	basicSharedConfig := initBasics(c, key, persistate)
	c.shared = shared{
		basics:         basicSharedConfig,
		localAddr:      netAddr,
		respSvc:        response.NewService(response.WithNetworkState(basicSharedConfig.networkState)),
		clientCache:    cache.NewSDKClientCache(cacheOpts),
		bgClientCache:  cache.NewSDKClientCache(cacheOpts),
		putClientCache: cache.NewSDKClientCache(cacheOpts),
		persistate:     persistate,
	}
	c.cfgContainer = cfgContainer{
		workerPool: containerWorkerPool,
	}
	c.cfgNetmap = cfgNetmap{
		state:         c.basics.networkState,
		workerPool:    netmapWorkerPool,
		needBootstrap: !relayOnly,
	}
	c.cfgGRPC = cfgGRPC{
		maxChunkSize:  maxChunkSize,
		maxAddrAmount: maxAddrAmount,
	}
	c.cfgMorph = cfgMorph{
		proxyScriptHash: contractsconfig.Proxy(appCfg),
	}
	c.cfgObject = cfgObject{
		pool:              initObjectPool(appCfg),
		tombstoneLifetime: objectconfig.TombstoneLifetime(appCfg),
	}
	c.cfgReputation = cfgReputation{
		workerPool: reputationWorkerPool,
	}

	c.cfgNetmap.reBoostrapTurnedOff.Store(nodeconfig.Relay(appCfg))

	c.ownerIDFromKey = user.NewFromECDSAPublicKey(key.PrivateKey.PublicKey)

	c.metricsCollector = metrics.NewNodeMetrics(misc.Version)
	c.basics.networkState.metrics = c.metricsCollector

	c.veryLastClosers = make(map[string]func())

	c.onShutdown(c.clientCache.CloseAll)    // clean up connections
	c.onShutdown(c.bgClientCache.CloseAll)  // clean up connections
	c.onShutdown(c.putClientCache.CloseAll) // clean up connections
	c.onShutdown(func() { _ = c.persistate.Close() })

	return c
}

func initBasics(c *cfg, key *keys.PrivateKey, stateStorage *state.PersistentStorage) basics {
	b := basics{}

	addresses := c.applicationConfiguration.morph.endpoints

	fromSideChainBlock, err := stateStorage.UInt32(persistateSideChainLastBlockKey)
	if err != nil {
		fromSideChainBlock = 0
		c.log.Warn("can't get last processed side chain block number", zap.String("error", err.Error()))
	}

	cli, err := client.New(key,
		client.WithContext(c.internals.ctx),
		client.WithDialTimeout(c.applicationConfiguration.morph.dialTimeout),
		client.WithLogger(c.log),
		client.WithAutoSidechainScope(),
		client.WithEndpoints(addresses),
		client.WithReconnectionRetries(c.applicationConfiguration.morph.reconnectionRetriesNumber),
		client.WithReconnectionsDelay(c.applicationConfiguration.morph.reconnectionRetriesDelay),
		client.WithConnSwitchCallback(func() {
			err = c.restartMorph()
			if err != nil {
				c.internalErr <- fmt.Errorf("restarting after morph connection was lost: %w", err)
			}
		}),
		client.WithMinRequiredBlockHeight(fromSideChainBlock),
	)
	if err != nil {
		c.log.Info("failed to create neo RPC client",
			zap.Any("endpoints", addresses),
			zap.String("error", err.Error()),
		)

		fatalOnErr(err)
	}

	lookupScriptHashesInNNS(cli, c.applicationConfiguration, &b)

	nState := newNetworkState()

	cnrWrap, err := cntClient.NewFromMorph(cli, b.containerSH, 0)
	fatalOnErr(err)

	cnrSrc := cntClient.AsContainerSource(cnrWrap)

	eACLFetcher := &morphEACLFetcher{
		w: cnrWrap,
	}

	nmWrap, err := nmClient.NewFromMorph(cli, b.netmapSH, 0)
	fatalOnErr(err)

	ttl := c.applicationConfiguration.morph.cacheTTL
	if ttl == 0 {
		msPerBlock, err := cli.MsPerBlock()
		fatalOnErr(err)
		ttl = time.Duration(msPerBlock) * time.Millisecond
		c.log.Debug("morph.cache_ttl fetched from network", zap.Duration("value", ttl))
	}

	var netmapSource netmapCore.Source
	if ttl < 0 {
		netmapSource = nmWrap
	} else {
		b.netmapCache = newCachedNetmapStorage(nState, nmWrap)
		b.containerCache = newCachedContainerStorage(cnrSrc, ttl)
		b.eaclCache = newCachedEACLStorage(eACLFetcher, ttl)
		b.containerListCache = newCachedContainerLister(cnrWrap, ttl)

		// use RPC node as source of netmap (with caching)
		netmapSource = b.netmapCache
	}

	b.netMapSource = netmapSource
	b.networkState = nState
	b.key = key
	b.binPublicKey = key.PublicKey().Bytes()
	b.cli = cli
	b.nCli = nmWrap
	b.cCli = cnrWrap
	b.ttl = ttl

	return b
}

func (c *cfg) policerOpts() []policer.Option {
	pCfg := c.applicationConfiguration.policer

	return []policer.Option{
		policer.WithMaxCapacity(pCfg.maxCapacity),
		policer.WithHeadTimeout(pCfg.headTimeout),
		policer.WithReplicationCooldown(pCfg.replicationCooldown),
		policer.WithObjectBatchSize(pCfg.objectBatchSize),
	}
}

func (c *cfg) LocalAddress() network.AddressGroup {
	return c.localAddr
}

func initObjectPool(cfg *config.Config) (pool cfgObjectRoutines) {
	var err error

	optNonBlocking := ants.WithNonblocking(true)

	pool.putRemoteCapacity = objectconfig.Put(cfg).PoolSizeRemote()

	pool.putRemote, err = ants.NewPool(pool.putRemoteCapacity, optNonBlocking)
	fatalOnErr(err)

	pool.putLocal, err = ants.NewPool(-1) // -1 stands for an infinite capacity worker
	fatalOnErr(err)

	pool.replicatorPoolSize = replicatorconfig.PoolSize(cfg)
	if pool.replicatorPoolSize <= 0 {
		pool.replicatorPoolSize = pool.putRemoteCapacity
	}

	pool.replication, err = ants.NewPool(pool.replicatorPoolSize)
	fatalOnErr(err)

	return pool
}

func (c *cfg) reloadObjectPoolSizes() {
	c.cfgObject.poolLock.Lock()
	defer c.cfgObject.poolLock.Unlock()

	c.cfgObject.pool.putRemoteCapacity = objectconfig.Put(c.cfgReader).PoolSizeRemote()
	c.cfgObject.pool.putRemote.Tune(c.cfgObject.pool.putRemoteCapacity)

	c.cfgObject.pool.replicatorPoolSize = replicatorconfig.PoolSize(c.cfgReader)
	if c.cfgObject.pool.replicatorPoolSize <= 0 {
		c.cfgObject.pool.replicatorPoolSize = c.cfgObject.pool.putRemoteCapacity
	}
	c.cfgObject.pool.replication.Tune(c.cfgObject.pool.replicatorPoolSize)
}

func (c *cfg) LocalNodeInfo() (*netmapV2.NodeInfo, error) {
	c.cfgNodeInfo.localInfoLock.RLock()
	defer c.cfgNodeInfo.localInfoLock.RUnlock()

	var res netmapV2.NodeInfo
	c.cfgNodeInfo.localInfo.WriteToV2(&res)

	return &res, nil
}

// handleLocalNodeInfoFromNetwork rewrites cached node info from the NeoFS network map.
// Called with nil when storage node is outside the NeoFS network map
// (before entering the network and after leaving it).
func (c *cfg) handleLocalNodeInfoFromNetwork(ni *netmap.NodeInfo) {
	c.cfgNetmap.state.setNodeInfo(ni)
	c.localNodeInNetmap.Store(ni != nil)
}

// bootstrapWithState calls "addPeer" method of the Sidechain Netmap contract
// with the binary-encoded information from the current node's configuration.
// The state is set using the provided setter which MUST NOT be nil.
func (c *cfg) bootstrapWithState(stateSetter func(*netmap.NodeInfo)) error {
	c.cfgNodeInfo.localInfoLock.RLock()
	ni := c.cfgNodeInfo.localInfo
	c.cfgNodeInfo.localInfoLock.RUnlock()
	stateSetter(&ni)

	prm := nmClient.AddPeerPrm{}
	prm.SetNodeInfo(ni)

	return c.cfgNetmap.wrapper.AddPeer(prm)
}

// bootstrapOnline calls cfg.bootstrapWithState with "online" state.
func bootstrapOnline(c *cfg) error {
	return c.bootstrapWithState((*netmap.NodeInfo).SetOnline)
}

// bootstrap calls bootstrapWithState with:
//   - "maintenance" state if maintenance is in progress on the current node
//   - "online", otherwise
func (c *cfg) bootstrap() error {
	// switch to online except when under maintenance
	st := c.cfgNetmap.state.controlNetmapStatus()
	if st == control.NetmapStatus_MAINTENANCE {
		c.log.Info("bootstrapping with the maintenance state")
		return c.bootstrapWithState((*netmap.NodeInfo).SetMaintenance)
	}

	c.log.Info("bootstrapping with online state",
		zap.Stringer("previous", st),
	)

	return bootstrapOnline(c)
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
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)

	for {
		select {
		case <-ch:
			c.log.Info("SIGHUP has been received, rereading configuration...")

			err := c.readConfig(c.cfgReader)
			if err != nil {
				c.log.Error("configuration reading", zap.Error(err))
				continue
			}

			// Pool

			c.reloadObjectPoolSizes()

			// Logger

			err = c.internals.logLevel.UnmarshalText([]byte(c.logger.level))
			if err != nil {
				c.log.Error("invalid logger level configuration", zap.Error(err))
				continue
			}

			// Policer

			c.shared.policer.Reload(c.policerOpts()...)

			// Storage Engine

			var rcfg engine.ReConfiguration
			for _, optsWithID := range c.shardOpts() {
				rcfg.AddShard(optsWithID.configID, optsWithID.shOpts)
			}
			rcfg.SetShardPoolSize(c.engine.shardPoolSize)

			err = c.cfgObject.cfgLocalStorage.localStorage.Reload(rcfg)
			if err != nil {
				c.log.Error("storage engine configuration update", zap.Error(err))
				continue
			}

			// Morph

			c.cli.Reload(client.WithEndpoints(c.morph.endpoints))

			// Node

			err = c.reloadNodeAttributes()
			if err != nil {
				c.log.Error("invalid node attributes configuration", zap.Error(err))
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
	for _, sh := range c.applicationConfiguration.engine.shards {
		for _, subStorage := range sh.SubStorages {
			path := subStorage.Path

			for len(path) > 1 { // Dir returns / or . if nothing else left.
				fi, err := os.Stat(path)
				if err == nil && fi.IsDir() {
					break
				}
				if err != nil && !errors.Is(err, fs.ErrNotExist) {
					return fmt.Errorf("accessing %q: %w", path, err)
				}
				path = filepath.Dir(path)
			}

			paths = append(paths, path)
		}
	}

	total, err := totalBytes(paths)
	if err != nil {
		return fmt.Errorf("calculating capacity on every shard: %w", err)
	}

	c.cfgNodeInfo.localInfo.SetCapacity(total / (1 << 30))

	return nil
}
