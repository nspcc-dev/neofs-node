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
	"strings"
	"sync"
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
	loggerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/logger"
	metricsconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/metrics"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	objectconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/object"
	replicatorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/replicator"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	shardmode "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
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
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/panjf2000/ants/v2"
	"go.etcd.io/bbolt"
	"go.uber.org/atomic"
	"go.uber.org/zap"
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
		shards         []shardCfg
	}
}

type shardCfg struct {
	compress                  bool
	smallSizeObjectLimit      uint64
	uncompressableContentType []string
	refillMetabase            bool
	mode                      shardmode.Mode

	metaCfg struct {
		path          string
		perm          fs.FileMode
		maxBatchSize  int
		maxBatchDelay time.Duration
	}

	subStorages []subStorageCfg

	gcCfg struct {
		removerBatchSize     int
		removerSleepInterval time.Duration
	}

	writecacheCfg struct {
		enabled          bool
		path             string
		maxBatchSize     int
		maxBatchDelay    time.Duration
		smallObjectSize  uint64
		maxObjSize       uint64
		flushWorkerCount int
		sizeLimit        uint64
		noSync           bool
	}

	piloramaCfg struct {
		enabled       bool
		path          string
		perm          fs.FileMode
		noSync        bool
		maxBatchSize  int
		maxBatchDelay time.Duration
	}
}

// id returns persistent id of a shard. It is different from the ID used in runtime
// and is primarily used to identify shards in the configuration.
func (c *shardCfg) id() string {
	// This calculation should be kept in sync with
	// pkg/local_object_storage/engine/control.go file.
	var sb strings.Builder
	for i := range c.subStorages {
		sb.WriteString(filepath.Clean(c.subStorages[i].path))
	}
	return sb.String()
}

type subStorageCfg struct {
	// common for all storages
	typ    string
	path   string
	perm   fs.FileMode
	depth  uint64
	noSync bool

	// blobovnicza-specific
	size            uint64
	width           uint64
	openedCacheSize int
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
		var sh shardCfg

		sh.refillMetabase = sc.RefillMetabase()
		sh.mode = sc.Mode()
		sh.compress = sc.Compress()
		sh.uncompressableContentType = sc.UncompressableContentTypes()
		sh.smallSizeObjectLimit = sc.SmallSizeLimit()

		// write-cache

		writeCacheCfg := sc.WriteCache()
		if writeCacheCfg.Enabled() {
			wc := &sh.writecacheCfg

			wc.enabled = true
			wc.path = writeCacheCfg.Path()
			wc.maxBatchSize = writeCacheCfg.BoltDB().MaxBatchSize()
			wc.maxBatchDelay = writeCacheCfg.BoltDB().MaxBatchDelay()
			wc.maxObjSize = writeCacheCfg.MaxObjectSize()
			wc.smallObjectSize = writeCacheCfg.SmallObjectSize()
			wc.flushWorkerCount = writeCacheCfg.WorkersNumber()
			wc.sizeLimit = writeCacheCfg.SizeLimit()
			wc.noSync = writeCacheCfg.NoSync()
		}

		// blobstor with substorages

		blobStorCfg := sc.BlobStor()
		storagesCfg := blobStorCfg.Storages()
		metabaseCfg := sc.Metabase()
		gcCfg := sc.GC()

		if config.BoolSafe(c.Sub("tree"), "enabled") {
			piloramaCfg := sc.Pilorama()
			pr := &sh.piloramaCfg

			pr.enabled = true
			pr.path = piloramaCfg.Path()
			pr.perm = piloramaCfg.Perm()
			pr.noSync = piloramaCfg.NoSync()
			pr.maxBatchSize = piloramaCfg.MaxBatchSize()
			pr.maxBatchDelay = piloramaCfg.MaxBatchDelay()
		}

		ss := make([]subStorageCfg, 0, len(storagesCfg))
		for i := range storagesCfg {
			var sCfg subStorageCfg

			sCfg.typ = storagesCfg[i].Type()
			sCfg.path = storagesCfg[i].Path()
			sCfg.perm = storagesCfg[i].Perm()

			switch storagesCfg[i].Type() {
			case blobovniczatree.Type:
				sub := blobovniczaconfig.From((*config.Config)(storagesCfg[i]))

				sCfg.size = sub.Size()
				sCfg.depth = sub.ShallowDepth()
				sCfg.width = sub.ShallowWidth()
				sCfg.openedCacheSize = sub.OpenedCacheSize()
			case fstree.Type:
				sub := fstreeconfig.From((*config.Config)(storagesCfg[i]))
				sCfg.depth = sub.Depth()
				sCfg.noSync = sub.NoSync()
			default:
				return fmt.Errorf("invalid storage type: %s", storagesCfg[i].Type())
			}

			ss = append(ss, sCfg)
		}

		sh.subStorages = ss

		// meta

		m := &sh.metaCfg

		m.path = metabaseCfg.Path()
		m.perm = metabaseCfg.BoltDB().Perm()
		m.maxBatchDelay = metabaseCfg.BoltDB().MaxBatchDelay()
		m.maxBatchSize = metabaseCfg.BoltDB().MaxBatchSize()

		// GC

		sh.gcCfg.removerBatchSize = gcCfg.RemoverBatchSize()
		sh.gcCfg.removerSleepInterval = gcCfg.RemoverSleepInterval()

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

	log *logger.Logger

	wg      *sync.WaitGroup
	workers []worker
	closers []func()

	apiVersion   version.Version
	healthStatus *atomic.Int32
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

// dynamicConfiguration stores parameters of the
// components that supports runtime reconfigurations.
type dynamicConfiguration struct {
	logger *logger.Prm
}

type cfg struct {
	applicationConfiguration
	internals
	shared
	dynamicConfiguration

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
	reBoostrapTurnedOff *atomic.Bool // managed by control service in runtime
	startEpoch          uint64       // epoch number when application is started
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

	key := nodeconfig.Key(appCfg)

	logPrm, err := c.loggerPrm()
	fatalOnErr(err)

	log, err := logger.NewLogger(logPrm)
	fatalOnErr(err)

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
		ctx:          context.Background(),
		appCfg:       appCfg,
		internalErr:  make(chan error, 10), // We only need one error, but we can have multiple senders.
		log:          log,
		wg:           new(sync.WaitGroup),
		apiVersion:   version.Current(),
		healthStatus: atomic.NewInt32(int32(control.HealthStatus_HEALTH_STATUS_UNDEFINED)),
	}

	cacheOpts := cache.ClientCacheOpts{
		DialTimeout:      apiclientconfig.DialTimeout(appCfg),
		StreamTimeout:    apiclientconfig.StreamTimeout(appCfg),
		Key:              &key.PrivateKey,
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
		scriptHash:          contractsconfig.Netmap(appCfg),
		state:               netState,
		workerPool:          netmapWorkerPool,
		needBootstrap:       !relayOnly,
		reBoostrapTurnedOff: atomic.NewBool(relayOnly),
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

	user.IDFromKey(&c.ownerIDFromKey, key.PrivateKey.PublicKey)

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
		if wcRead := shCfg.writecacheCfg; wcRead.enabled {
			writeCacheOpts = append(writeCacheOpts,
				writecache.WithPath(wcRead.path),
				writecache.WithMaxBatchSize(wcRead.maxBatchSize),
				writecache.WithMaxBatchDelay(wcRead.maxBatchDelay),
				writecache.WithMaxObjectSize(wcRead.maxObjSize),
				writecache.WithSmallObjectSize(wcRead.smallObjectSize),
				writecache.WithFlushWorkersCount(wcRead.flushWorkerCount),
				writecache.WithMaxCacheSize(wcRead.sizeLimit),
				writecache.WithNoSync(wcRead.noSync),
				writecache.WithLogger(c.log),
			)
		}

		var piloramaOpts []pilorama.Option
		if prRead := shCfg.piloramaCfg; prRead.enabled {
			piloramaOpts = append(piloramaOpts,
				pilorama.WithPath(prRead.path),
				pilorama.WithPerm(prRead.perm),
				pilorama.WithNoSync(prRead.noSync),
				pilorama.WithMaxBatchSize(prRead.maxBatchSize),
				pilorama.WithMaxBatchDelay(prRead.maxBatchDelay),
			)
		}

		var ss []blobstor.SubStorage
		for _, sRead := range shCfg.subStorages {
			switch sRead.typ {
			case blobovniczatree.Type:
				ss = append(ss, blobstor.SubStorage{
					Storage: blobovniczatree.NewBlobovniczaTree(
						blobovniczatree.WithRootPath(sRead.path),
						blobovniczatree.WithPermissions(sRead.perm),
						blobovniczatree.WithBlobovniczaSize(sRead.size),
						blobovniczatree.WithBlobovniczaShallowDepth(sRead.depth),
						blobovniczatree.WithBlobovniczaShallowWidth(sRead.width),
						blobovniczatree.WithOpenedCacheSize(sRead.openedCacheSize),

						blobovniczatree.WithLogger(c.log)),
					Policy: func(_ *objectSDK.Object, data []byte) bool {
						return uint64(len(data)) < shCfg.smallSizeObjectLimit
					},
				})
			case fstree.Type:
				ss = append(ss, blobstor.SubStorage{
					Storage: fstree.New(
						fstree.WithPath(sRead.path),
						fstree.WithPerm(sRead.perm),
						fstree.WithDepth(sRead.depth),
						fstree.WithNoSync(sRead.noSync)),
					Policy: func(_ *objectSDK.Object, data []byte) bool {
						return true
					},
				})
			default:
				// should never happen, that has already
				// been handled: when the config was read
			}
		}

		var sh shardOptsWithID
		sh.configID = shCfg.id()
		sh.shOpts = []shard.Option{
			shard.WithLogger(c.log),
			shard.WithRefillMetabase(shCfg.refillMetabase),
			shard.WithMode(shCfg.mode),
			shard.WithBlobStorOptions(
				blobstor.WithCompressObjects(shCfg.compress),
				blobstor.WithUncompressableContentTypes(shCfg.uncompressableContentType),
				blobstor.WithStorages(ss),

				blobstor.WithLogger(c.log),
			),
			shard.WithMetaBaseOptions(
				meta.WithPath(shCfg.metaCfg.path),
				meta.WithPermissions(shCfg.metaCfg.perm),
				meta.WithMaxBatchSize(shCfg.metaCfg.maxBatchSize),
				meta.WithMaxBatchDelay(shCfg.metaCfg.maxBatchDelay),
				meta.WithBoltDBOptions(&bbolt.Options{
					Timeout: 100 * time.Millisecond,
				}),

				meta.WithLogger(c.log),
				meta.WithEpochState(c.cfgNetmap.state),
			),
			shard.WithPiloramaOptions(piloramaOpts...),
			shard.WithWriteCache(shCfg.writecacheCfg.enabled),
			shard.WithWriteCacheOptions(writeCacheOpts...),
			shard.WithRemoverBatchSize(shCfg.gcCfg.removerBatchSize),
			shard.WithGCRemoverSleepInterval(shCfg.gcCfg.removerSleepInterval),
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

func (c *cfg) loggerPrm() (*logger.Prm, error) {
	// check if it has been inited before
	if c.dynamicConfiguration.logger == nil {
		c.dynamicConfiguration.logger = new(logger.Prm)
	}

	// (re)init read configuration
	err := c.dynamicConfiguration.logger.SetLevelString(c.LoggerCfg.level)
	if err != nil {
		// not expected since validation should be performed before
		panic(fmt.Sprintf("incorrect log level format: %s", c.LoggerCfg.level))
	}

	return c.dynamicConfiguration.logger, nil
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
	ni := c.cfgNodeInfo.localInfo
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

type dCfg struct {
	name string
	cfg  interface {
		Reload() error
	}
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

			// all the components are expected to support
			// Logger's dynamic reconfiguration approach
			var components []dCfg

			// Logger

			logPrm, err := c.loggerPrm()
			if err != nil {
				c.log.Error("logger configuration preparation", zap.Error(err))
				continue
			}

			components = append(components, dCfg{name: "logger", cfg: logPrm})

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

			for _, component := range components {
				err = component.cfg.Reload()
				if err != nil {
					c.log.Error("updated configuration applying",
						zap.String("component", component.name),
						zap.Error(err))
				}
			}

			c.log.Info("configuration has been reloaded successfully")
		case <-ctx.Done():
			return
		}
	}
}
