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
	atomicstd "sync/atomic"
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
	blobovniczaconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/blobovnicza"
	fstreeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/fstree"
	peapodconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/peapod"
	loggerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/logger"
	metricsconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/metrics"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	objectconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/object"
	replicatorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/replicator"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/storage"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/metrics"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	containerClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmap2 "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/tombstone"
	tsourse "github.com/nspcc-dev/neofs-node/pkg/services/object_manager/tombstone/source"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/panjf2000/ants/v2"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

const addressSize = 72 // 32 bytes object ID, 32 bytes container ID, 8 bytes protobuf encoding

const maxMsgSize = 4 << 20 // transport msg limit 4 MiB

// capacity of the pools of the morph notification handlers
// for each contract listener.
const notificationHandlerPoolSize = 10

// applicationConfiguration reads and stores component-specific configuration
// values. It should not store any application helpers structs (pointers to shared
// structs).
// It must not be used concurrently.
type applicationConfiguration struct {
	// _read indicated whether a config
	// has already been read
	_read bool

	LoggerCfg struct {
		level string
	}

	EngineCfg struct {
		errorThreshold uint32
		shardPoolSize  uint32
		shards         []storage.ShardCfg
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

	a.LoggerCfg.level = loggerconfig.Level(c)

	// Storage Engine

	a.EngineCfg.errorThreshold = engineconfig.ShardErrorThreshold(c)
	a.EngineCfg.shardPoolSize = engineconfig.ShardPoolSize(c)

	return engineconfig.IterateShards(c, false, func(sc *shardconfig.Config) error {
		var sh storage.ShardCfg

		sh.RefillMetabase = sc.RefillMetabase()
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

			switch storagesCfg[i].Type() {
			case blobovniczatree.Type:
				sub := blobovniczaconfig.From((*config.Config)(storagesCfg[i]))

				sCfg.Size = sub.Size()
				sCfg.Depth = sub.ShallowDepth()
				sCfg.Width = sub.ShallowWidth()
				sCfg.OpenedCacheSize = sub.OpenedCacheSize()
			case fstree.Type:
				sub := fstreeconfig.From((*config.Config)(storagesCfg[i]))
				sCfg.Depth = sub.Depth()
				sCfg.NoSync = sub.NoSync()
			case peapod.Type:
				peapodCfg := peapodconfig.From((*config.Config)(storagesCfg[i]))
				sCfg.FlushInterval = peapodCfg.FlushInterval()
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

		a.EngineCfg.shards = append(a.EngineCfg.shards, sh)

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

	appCfg *config.Config

	logLevel zap.AtomicLevel
	log      *zap.Logger

	wg      *sync.WaitGroup
	workers []worker
	closers []func()

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

// shared contains component-specific structs/helpers that should
// be shared during initialization of the application.
type shared struct {
	privateTokenStore sessionStorage
	persistate        *state.PersistentStorage

	clientCache    *cache.ClientCache
	bgClientCache  *cache.ClientCache
	putClientCache *cache.ClientCache
	localAddr      network.AddressGroup

	containerCache     *ttlContainerStorage
	eaclCache          *ttlEACLStorage
	containerListCache *ttlContainerLister
	netmapCache        *lruNetmapSource

	key            *keys.PrivateKey
	binPublicKey   []byte
	ownerIDFromKey user.ID // user ID calculated from key

	// current network map
	netMap       atomicstd.Value // type netmap.NetMap
	netMapSource netmapCore.Source

	// whether the local node is in the netMap
	localNodeInNetmap atomic.Bool

	cnrClient *containerClient.Client

	respSvc *response.Service

	replicator *replicator.Replicator

	treeService *tree.Service

	metricsCollector *metrics.NodeMetrics
}

func (s shared) resetCaches() {
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
	cfgAccounting     cfgAccounting
	cfgContainer      cfgContainer
	cfgNodeInfo       cfgNodeInfo
	cfgNetmap         cfgNetmap
	cfgControlService cfgControlService
	cfgReputation     cfgReputation
	cfgObject         cfgObject
	cfgNotifications  cfgNotifications
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

	// TTL of Sidechain cached values. Non-positive value disables caching.
	cacheTTL time.Duration

	eigenTrustTicker *eigenTrustTickers // timers for EigenTrust iterations

	proxyScriptHash neogoutil.Uint160
}

type cfgAccounting struct {
	scriptHash neogoutil.Uint160
}

type cfgContainer struct {
	scriptHash neogoutil.Uint160

	parsers     map[event.Type]event.NotificationParser
	subscribers map[event.Type][]event.Handler
	workerPool  util.WorkerPool // pool for asynchronous handlers
}

type cfgNetmap struct {
	scriptHash neogoutil.Uint160
	wrapper    *nmClient.Client

	parsers map[event.Type]event.NotificationParser

	subscribers map[event.Type][]event.Handler
	workerPool  util.WorkerPool // pool for asynchronous handlers

	state *networkState

	needBootstrap       bool
	reBoostrapTurnedOff atomic.Bool // managed by control service in runtime
	startEpoch          uint64      // epoch number when application is started
}

type cfgNodeInfo struct {
	// values from config
	localInfo netmap.NodeInfo
}

type cfgObject struct {
	getSvc *getsvc.Service

	cnrSource container.Source

	eaclSource container.EACLSource

	pool cfgObjectRoutines

	cfgLocalStorage cfgLocalStorage

	tombstoneLifetime uint64
}

type cfgNotifications struct {
	enabled      bool
	nw           notificationWriter
	defaultTopic string
}

type cfgLocalStorage struct {
	localStorage *engine.StorageEngine
}

type cfgObjectRoutines struct {
	putRemote *ants.Pool

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

	scriptHash neogoutil.Uint160
}

var persistateSideChainLastBlockKey = []byte("side_chain_last_processed_block")

func initCfg(appCfg *config.Config) *cfg {
	c := &cfg{}

	err := c.readConfig(appCfg)
	if err != nil {
		panic(fmt.Errorf("config reading: %w", err))
	}

	// filling system attributes; do not move it anywhere
	// below applying the other attributes since a user
	// should be able to overwrite it.
	err = writeSystemAttributes(c)
	fatalOnErr(err)

	key := nodeconfig.Key(appCfg)

	var netAddr network.AddressGroup

	relayOnly := nodeconfig.Relay(appCfg)
	if !relayOnly {
		netAddr = nodeconfig.BootstrapAddresses(appCfg)
	}

	maxChunkSize := uint64(maxMsgSize) * 3 / 4          // 25% to meta, 75% to payload
	maxAddrAmount := uint64(maxChunkSize) / addressSize // each address is about 72 bytes

	netState := newNetworkState()

	persistate, err := state.NewPersistentStorage(nodeconfig.PersistentState(appCfg).Path())
	fatalOnErr(err)

	containerWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	netmapWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	reputationWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	c.internals = internals{
		ctx:         context.Background(),
		appCfg:      appCfg,
		internalErr: make(chan error, 10), // We only need one error, but we can have multiple senders.
		wg:          new(sync.WaitGroup),
		apiVersion:  version.Current(),
	}
	c.internals.healthStatus.Store(int32(control.HealthStatus_HEALTH_STATUS_UNDEFINED))

	c.internals.logLevel, err = zap.ParseAtomicLevel(c.LoggerCfg.level)
	fatalOnErr(err)

	logCfg := zap.NewProductionConfig()
	logCfg.Level = c.internals.logLevel
	logCfg.Encoding = "console"
	logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	c.internals.log, err = logCfg.Build(
		zap.AddStacktrace(zap.NewAtomicLevelAt(zap.FatalLevel)),
	)
	fatalOnErr(err)

	cacheOpts := cache.ClientCacheOpts{
		DialTimeout:      apiclientconfig.DialTimeout(appCfg),
		StreamTimeout:    apiclientconfig.StreamTimeout(appCfg),
		AllowExternal:    apiclientconfig.AllowExternal(appCfg),
		ReconnectTimeout: apiclientconfig.ReconnectTimeout(appCfg),
	}
	c.shared = shared{
		key:            key,
		binPublicKey:   key.PublicKey().Bytes(),
		localAddr:      netAddr,
		respSvc:        response.NewService(response.WithNetworkState(netState)),
		clientCache:    cache.NewSDKClientCache(cacheOpts),
		bgClientCache:  cache.NewSDKClientCache(cacheOpts),
		putClientCache: cache.NewSDKClientCache(cacheOpts),
		persistate:     persistate,
	}
	c.cfgAccounting = cfgAccounting{
		scriptHash: contractsconfig.Balance(appCfg),
	}
	c.cfgContainer = cfgContainer{
		scriptHash: contractsconfig.Container(appCfg),
		workerPool: containerWorkerPool,
	}
	c.cfgNetmap = cfgNetmap{
		scriptHash:    contractsconfig.Netmap(appCfg),
		state:         netState,
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
		scriptHash: contractsconfig.Reputation(appCfg),
		workerPool: reputationWorkerPool,
	}

	c.cfgNetmap.reBoostrapTurnedOff.Store(nodeconfig.Relay(appCfg))

	c.ownerIDFromKey = user.ResolveFromECDSAPublicKey(key.PrivateKey.PublicKey)

	if metricsconfig.Enabled(c.appCfg) {
		c.metricsCollector = metrics.NewNodeMetrics(misc.Version)
		netState.metrics = c.metricsCollector
	}

	c.onShutdown(c.clientCache.CloseAll)    // clean up connections
	c.onShutdown(c.bgClientCache.CloseAll)  // clean up connections
	c.onShutdown(c.putClientCache.CloseAll) // clean up connections
	c.onShutdown(func() { _ = c.persistate.Close() })

	return c
}

func (c *cfg) engineOpts() []engine.Option {
	opts := make([]engine.Option, 0, 4)

	opts = append(opts,
		engine.WithShardPoolSize(c.EngineCfg.shardPoolSize),
		engine.WithErrorThreshold(c.EngineCfg.errorThreshold),

		engine.WithLogger(c.log),
	)

	if c.metricsCollector != nil {
		opts = append(opts, engine.WithMetrics(c.metricsCollector))
	}

	return opts
}

type shardOptsWithID struct {
	configID string
	shOpts   []shard.Option
}

func (c *cfg) shardOpts() []shardOptsWithID {
	shards := make([]shardOptsWithID, 0, len(c.EngineCfg.shards))

	for _, shCfg := range c.EngineCfg.shards {
		var writeCacheOpts []writecache.Option
		if wcRead := shCfg.WritecacheCfg; wcRead.Enabled {
			writeCacheOpts = append(writeCacheOpts,
				writecache.WithPath(wcRead.Path),
				writecache.WithMaxBatchSize(wcRead.MaxBatchSize),
				writecache.WithMaxBatchDelay(wcRead.MaxBatchDelay),
				writecache.WithMaxObjectSize(wcRead.MaxObjSize),
				writecache.WithSmallObjectSize(wcRead.SmallObjectSize),
				writecache.WithFlushWorkersCount(wcRead.FlushWorkerCount),
				writecache.WithMaxCacheSize(wcRead.SizeLimit),
				writecache.WithNoSync(wcRead.NoSync),
				writecache.WithLogger(c.log),
			)
		}

		var piloramaOpts []pilorama.Option
		if prRead := shCfg.PiloramaCfg; prRead.Enabled {
			piloramaOpts = append(piloramaOpts,
				pilorama.WithPath(prRead.Path),
				pilorama.WithPerm(prRead.Perm),
				pilorama.WithNoSync(prRead.NoSync),
				pilorama.WithMaxBatchSize(prRead.MaxBatchSize),
				pilorama.WithMaxBatchDelay(prRead.MaxBatchDelay),
			)
		}

		var ss []blobstor.SubStorage
		for _, sRead := range shCfg.SubStorages {
			switch sRead.Typ {
			case blobovniczatree.Type:
				ss = append(ss, blobstor.SubStorage{
					Storage: blobovniczatree.NewBlobovniczaTree(
						blobovniczatree.WithRootPath(sRead.Path),
						blobovniczatree.WithPermissions(sRead.Perm),
						blobovniczatree.WithBlobovniczaSize(sRead.Size),
						blobovniczatree.WithBlobovniczaShallowDepth(sRead.Depth),
						blobovniczatree.WithBlobovniczaShallowWidth(sRead.Width),
						blobovniczatree.WithOpenedCacheSize(sRead.OpenedCacheSize),

						blobovniczatree.WithLogger(c.log)),
					Policy: func(_ *objectSDK.Object, data []byte) bool {
						return uint64(len(data)) < shCfg.SmallSizeObjectLimit
					},
				})
			case fstree.Type:
				ss = append(ss, blobstor.SubStorage{
					Storage: fstree.New(
						fstree.WithPath(sRead.Path),
						fstree.WithPerm(sRead.Perm),
						fstree.WithDepth(sRead.Depth),
						fstree.WithNoSync(sRead.NoSync)),
					Policy: func(_ *objectSDK.Object, data []byte) bool {
						return true
					},
				})
			case peapod.Type:
				ss = append(ss, blobstor.SubStorage{
					Storage: peapod.New(sRead.Path, sRead.Perm, sRead.FlushInterval),
					Policy: func(_ *objectSDK.Object, data []byte) bool {
						return uint64(len(data)) < shCfg.SmallSizeObjectLimit
					},
				})
			default:
				// should never happen, that has already
				// been handled: when the config was read
			}
		}

		var sh shardOptsWithID
		sh.configID = shCfg.ID()
		sh.shOpts = []shard.Option{
			shard.WithLogger(c.log),
			shard.WithRefillMetabase(shCfg.RefillMetabase),
			shard.WithMode(shCfg.Mode),
			shard.WithBlobStorOptions(
				blobstor.WithCompressObjects(shCfg.Compress),
				blobstor.WithUncompressableContentTypes(shCfg.UncompressableContentType),
				blobstor.WithStorages(ss),

				blobstor.WithLogger(c.log),
			),
			shard.WithMetaBaseOptions(
				meta.WithPath(shCfg.MetaCfg.Path),
				meta.WithPermissions(shCfg.MetaCfg.Perm),
				meta.WithMaxBatchSize(shCfg.MetaCfg.MaxBatchSize),
				meta.WithMaxBatchDelay(shCfg.MetaCfg.MaxBatchDelay),
				meta.WithBoltDBOptions(&bbolt.Options{
					Timeout: 100 * time.Millisecond,
				}),

				meta.WithLogger(c.log),
				meta.WithEpochState(c.cfgNetmap.state),
			),
			shard.WithPiloramaOptions(piloramaOpts...),
			shard.WithWriteCache(shCfg.WritecacheCfg.Enabled),
			shard.WithWriteCacheOptions(writeCacheOpts...),
			shard.WithRemoverBatchSize(shCfg.GcCfg.RemoverBatchSize),
			shard.WithGCRemoverSleepInterval(shCfg.GcCfg.RemoverSleepInterval),
			shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
				pool, err := ants.NewPool(sz)
				fatalOnErr(err)

				return pool
			}),
		}

		shards = append(shards, sh)
	}

	return shards
}

func (c *cfg) LocalAddress() network.AddressGroup {
	return c.localAddr
}

func initLocalStorage(c *cfg) {
	ls := engine.New(c.engineOpts()...)

	addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
		ls.HandleNewEpoch(ev.(netmap2.NewEpoch).EpochNumber())
	})

	// allocate memory for the service;
	// service will be created later
	c.cfgObject.getSvc = new(getsvc.Service)

	var tssPrm tsourse.TombstoneSourcePrm
	tssPrm.SetGetService(c.cfgObject.getSvc)
	tombstoneSrc := tsourse.NewSource(tssPrm)

	tombstoneSource := tombstone.NewChecker(
		tombstone.WithLogger(c.log),
		tombstone.WithTombstoneSource(tombstoneSrc),
	)

	var shardsAttached int
	for _, optsWithMeta := range c.shardOpts() {
		id, err := ls.AddShard(append(optsWithMeta.shOpts, shard.WithTombstoneSource(tombstoneSource))...)
		if err != nil {
			c.log.Error("failed to attach shard to engine", zap.Error(err))
		} else {
			shardsAttached++
			c.log.Info("shard attached to engine", zap.Stringer("id", id))
		}
	}
	if shardsAttached == 0 {
		fatalOnErr(engineconfig.ErrNoShardConfigured)
	}

	c.cfgObject.cfgLocalStorage.localStorage = ls

	c.onShutdown(func() {
		c.log.Info("closing components of the storage engine...")

		err := ls.Close()
		if err != nil {
			c.log.Info("storage engine closing failure",
				zap.String("error", err.Error()),
			)
		} else {
			c.log.Info("all components of the storage engine closed successfully")
		}
	})
}

func initObjectPool(cfg *config.Config) (pool cfgObjectRoutines) {
	var err error

	optNonBlocking := ants.WithNonblocking(true)

	pool.putRemoteCapacity = objectconfig.Put(cfg).PoolSizeRemote()

	pool.putRemote, err = ants.NewPool(pool.putRemoteCapacity, optNonBlocking)
	fatalOnErr(err)

	pool.replicatorPoolSize = replicatorconfig.PoolSize(cfg)
	if pool.replicatorPoolSize <= 0 {
		pool.replicatorPoolSize = pool.putRemoteCapacity
	}

	pool.replication, err = ants.NewPool(pool.replicatorPoolSize)
	fatalOnErr(err)

	return pool
}

func (c *cfg) LocalNodeInfo() (*netmapV2.NodeInfo, error) {
	var res netmapV2.NodeInfo

	ni, ok := c.cfgNetmap.state.getNodeInfo()
	if ok {
		ni.WriteToV2(&res)
	} else {
		c.cfgNodeInfo.localInfo.WriteToV2(&res)
	}

	return &res, nil
}

// handleLocalNodeInfo rewrites local node info from the NeoFS network map.
// Called with nil when storage node is outside the NeoFS network map
// (before entering the network and after leaving it).
func (c *cfg) handleLocalNodeInfo(ni *netmap.NodeInfo) {
	c.cfgNetmap.state.setNodeInfo(ni)
	c.localNodeInNetmap.Store(ni != nil)
}

// bootstrapWithState calls "addPeer" method of the Sidechain Netmap contract
// with the binary-encoded information from the current node's configuration.
// The state is set using the provided setter which MUST NOT be nil.
func (c *cfg) bootstrapWithState(stateSetter func(*netmap.NodeInfo)) error {
	var ni netmap.NodeInfo
	if niAtomic := c.cfgNetmap.state.nodeInfo.Load(); niAtomic != nil {
		// node has already been bootstrapped successfully
		ni = niAtomic.(netmap.NodeInfo)
	} else {
		// unknown network state
		ni = c.cfgNodeInfo.localInfo
	}

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
	return float64(c.cfgObject.pool.putRemote.Running()) / float64(c.cfgObject.pool.putRemoteCapacity)
}

func (c *cfg) configWatcher(ctx context.Context) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)

	for {
		select {
		case <-ch:
			c.log.Info("SIGHUP has been received, rereading configuration...")

			err := c.readConfig(c.appCfg)
			if err != nil {
				c.log.Error("configuration reading", zap.Error(err))
				continue
			}

			// Logger

			err = c.internals.logLevel.UnmarshalText([]byte(c.LoggerCfg.level))
			if err != nil {
				c.log.Error("invalid logger level configuration", zap.Error(err))
				continue
			}

			// Storage Engine

			var rcfg engine.ReConfiguration
			for _, optsWithID := range c.shardOpts() {
				rcfg.AddShard(optsWithID.configID, optsWithID.shOpts)
			}

			err = c.cfgObject.cfgLocalStorage.localStorage.Reload(rcfg)
			if err != nil {
				c.log.Error("storage engine configuration update", zap.Error(err))
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

	c.cfgNodeInfo.localInfo.SetAttribute("Version", misc.Version)

	// `Capacity` attribute

	var paths []string
	for _, sh := range c.applicationConfiguration.EngineCfg.shards {
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
