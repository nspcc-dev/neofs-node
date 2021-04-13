package main

import (
	"context"
	"crypto/ecdsa"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	netmapV2 "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/metrics"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	nmwrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmap2 "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	tokenStorage "github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
	util2 "github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-node/pkg/util/profiler"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.etcd.io/bbolt"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// logger keys
	cfgLogLevel              = "logger.level"
	cfgLogFormat             = "logger.format"
	cfgLogTrace              = "logger.trace_level"
	cfgLogInitSampling       = "logger.sampling.initial"
	cfgLogThereafterSampling = "logger.sampling.thereafter"

	// pprof keys
	cfgProfilerEnable = "pprof.enabled"
	cfgProfilerAddr   = "pprof.address"
	cfgProfilerTTL    = "pprof.shutdown_ttl"

	// metrics keys
	cfgMetricsEnable = "metrics.enabled"
	cfgMetricsAddr   = "metrics.address"

	// config keys for cfgNodeInfo
	cfgNodeKey             = "node.key"
	cfgBootstrapAddress    = "node.address"
	cfgNodeAttributePrefix = "node.attribute"

	// config keys for cfgGRPC
	cfgListenAddress  = "grpc.endpoint"
	cfgMaxMsgSize     = "grpc.maxmessagesize"
	cfgReflectService = "grpc.enable_reflect_service"
	cfgDialTimeout    = "grpc.dial_timeout"

	// config keys for cfgMorph
	cfgMorphRPCAddress = "morph.rpc_endpoint"

	cfgMorphNotifyRPCAddress  = "morph.notification_endpoint"
	cfgMorphNotifyDialTimeout = "morph.dial_timeout"

	// config keys for cfgAccounting
	cfgAccountingContract = "accounting.scripthash"
	cfgAccountingFee      = "accounting.fee"

	// config keys for cfgNetmap
	cfgNetmapContract          = "netmap.scripthash"
	cfgNetmapFee               = "netmap.fee"
	cfgNetmapWorkerPoolEnabled = "netmap.async_worker.enabled"
	cfgNetmapWorkerPoolSize    = "netmap.async_worker.size"

	// config keys for cfgContainer
	cfgContainerContract          = "container.scripthash"
	cfgContainerFee               = "container.fee"
	cfgContainerWorkerPoolEnabled = "container.async_worker.enabled"
	cfgContainerWorkerPoolSize    = "container.async_worker.size"

	cfgGCQueueSize = "gc.queuesize"
	cfgGCQueueTick = "gc.duration.sleep"
	cfgGCTimeout   = "gc.duration.timeout"

	cfgPolicerWorkScope   = "policer.work_scope"
	cfgPolicerExpRate     = "policer.expansion_rate"
	cfgPolicerHeadTimeout = "policer.head_timeout"

	cfgReplicatorPutTimeout = "replicator.put_timeout"

	cfgReBootstrapEnabled  = "bootstrap.periodic.enabled"
	cfgReBootstrapInterval = "bootstrap.periodic.interval"

	cfgShutdownOfflineEnabled = "shutdown.offline.enabled"

	cfgObjectPutPoolSize       = "pool.object.put.size"
	cfgObjectGetPoolSize       = "pool.object.get.size"
	cfgObjectHeadPoolSize      = "pool.object.head.size"
	cfgObjectSearchPoolSize    = "pool.object.search.size"
	cfgObjectRangePoolSize     = "pool.object.range.size"
	cfgObjectRangeHashPoolSize = "pool.object.rangehash.size"
)

const (
	cfgLocalStorageSection = "storage"
	cfgStorageShardSection = "shard"

	cfgShardUseWriteCache = "use_write_cache"

	cfgBlobStorSection      = "blobstor"
	cfgWriteCacheSection    = "writecache"
	cfgBlobStorCompress     = "compress"
	cfgBlobStorShallowDepth = "shallow_depth"
	cfgBlobStorTreePath     = "path"
	cfgBlobStorTreePerm     = "perm"
	cfgBlobStorSmallSzLimit = "small_size_limit"

	cfgBlobStorBlzSection = "blobovnicza"
	cfgBlzSize            = "size"
	cfgBlzShallowDepth    = "shallow_depth"
	cfgBlzShallowWidth    = "shallow_width"
	cfgBlzOpenedCacheSize = "opened_cache_size"

	cfgMetaBaseSection = "metabase"
	cfgMetaBasePath    = "path"
	cfgMetaBasePerm    = "perm"

	cfgGCSection          = "gc"
	cfgGCRemoverBatchSize = "remover_batch_size"
	cfgGCRemoverSleepInt  = "remover_sleep_interval"
)

const cfgTombstoneLifetime = "tombstone_lifetime"

const (
	addressSize = 72 // 32 bytes oid, 32 bytes cid, 8 bytes protobuf encoding
)

type cfg struct {
	ctx context.Context

	ctxCancel func()

	internalErr chan error // channel for internal application errors at runtime

	viper *viper.Viper

	log *zap.Logger

	wg *sync.WaitGroup

	key *ecdsa.PrivateKey

	apiVersion *pkg.Version

	cfgGRPC cfgGRPC

	cfgMorph cfgMorph

	cfgAccounting cfgAccounting

	cfgContainer cfgContainer

	cfgNetmap cfgNetmap

	privateTokenStore *tokenStorage.TokenStore

	cfgNodeInfo cfgNodeInfo

	localAddr *network.Address

	cfgObject cfgObject

	profiler profiler.Profiler

	metricsServer profiler.Metrics

	metricsCollector *metrics.StorageMetrics

	workers []worker

	respSvc *response.Service

	cfgControlService cfgControlService

	netStatus *atomic.Int32

	healthStatus *atomic.Int32

	closers []func()

	cfgReputation cfgReputation
}

type cfgGRPC struct {
	listener net.Listener

	server *grpc.Server

	maxChunkSize uint64

	maxAddrAmount uint64

	enableReflectService bool
}

type cfgMorph struct {
	client *client.Client
}

type cfgAccounting struct {
	scriptHash util.Uint160

	fee fixedn.Fixed8
}

type cfgContainer struct {
	scriptHash util.Uint160

	fee fixedn.Fixed8

	parsers     map[event.Type]event.Parser
	subscribers map[event.Type][]event.Handler
	workerPool  util2.WorkerPool // pool for asynchronous handlers
}

type cfgNetmap struct {
	scriptHash util.Uint160
	wrapper    *nmwrapper.Wrapper

	fee fixedn.Fixed8

	parsers map[event.Type]event.Parser

	subscribers map[event.Type][]event.Handler
	workerPool  util2.WorkerPool // pool for asynchronous handlers

	state *networkState

	reBootstrapEnabled  bool
	reBootstrapInterval uint64 // in epochs

	goOfflineEnabled bool // send `UpdateState(offline)` tx at shutdown
}

type BootstrapType uint32

type cfgNodeInfo struct {
	// values from config
	bootType   BootstrapType
	attributes []*netmap.NodeAttribute

	// values at runtime
	infoMtx sync.RWMutex
	info    netmap.NodeInfo
}

type cfgObject struct {
	netMapStorage netmapCore.Source

	cnrStorage container.Source

	cnrClient *wrapper.Wrapper

	pool cfgObjectRoutines

	cfgLocalStorage cfgLocalStorage
}

type cfgLocalStorage struct {
	localStorage *engine.StorageEngine

	shardOpts [][]shard.Option
}

type cfgObjectRoutines struct {
	get, head, put, search, rng, rngHash *ants.Pool
}

type cfgControlService struct {
	server *grpc.Server
}

type cfgReputation struct {
	localTrustStorage *truststorage.Storage

	localTrustCtrl *trustcontroller.Controller
}

const (
	_ BootstrapType = iota
	StorageNode
	RelayNode
)

func initCfg(path string) *cfg {
	viperCfg := initViper(path)

	key, err := crypto.LoadPrivateKey(viperCfg.GetString(cfgNodeKey))
	fatalOnErr(err)

	u160Accounting, err := util.Uint160DecodeStringLE(
		viperCfg.GetString(cfgAccountingContract))
	fatalOnErr(err)

	u160Netmap, err := util.Uint160DecodeStringLE(
		viperCfg.GetString(cfgNetmapContract))
	fatalOnErr(err)

	u160Container, err := util.Uint160DecodeStringLE(
		viperCfg.GetString(cfgContainerContract))
	fatalOnErr(err)

	log, err := logger.NewLogger(viperCfg)
	fatalOnErr(err)

	netAddr, err := network.AddressFromString(viperCfg.GetString(cfgBootstrapAddress))
	fatalOnErr(err)

	maxChunkSize := viperCfg.GetUint64(cfgMaxMsgSize) * 3 / 4 // 25% to meta, 75% to payload
	maxAddrAmount := maxChunkSize / addressSize               // each address is about 72 bytes

	state := newNetworkState()

	// initialize async workers if it is configured so
	containerWorkerPool, err := initContainerWorkerPool(viperCfg)
	fatalOnErr(err)

	netmapWorkerPool, err := initNetmapWorkerPool(viperCfg)
	fatalOnErr(err)

	c := &cfg{
		ctx:         context.Background(),
		internalErr: make(chan error),
		viper:       viperCfg,
		log:         log,
		wg:          new(sync.WaitGroup),
		key:         key,
		apiVersion:  pkg.SDKVersion(),
		cfgAccounting: cfgAccounting{
			scriptHash: u160Accounting,
			fee:        fixedn.Fixed8(viperCfg.GetInt(cfgAccountingFee)),
		},
		cfgContainer: cfgContainer{
			scriptHash: u160Container,
			fee:        fixedn.Fixed8(viperCfg.GetInt(cfgContainerFee)),
			workerPool: containerWorkerPool,
		},
		cfgNetmap: cfgNetmap{
			scriptHash:          u160Netmap,
			fee:                 fixedn.Fixed8(viperCfg.GetInt(cfgNetmapFee)),
			state:               state,
			workerPool:          netmapWorkerPool,
			reBootstrapInterval: viperCfg.GetUint64(cfgReBootstrapInterval),
			reBootstrapEnabled:  viperCfg.GetBool(cfgReBootstrapEnabled),
			goOfflineEnabled:    viperCfg.GetBool(cfgShutdownOfflineEnabled),
		},
		cfgNodeInfo: cfgNodeInfo{
			bootType:   StorageNode,
			attributes: parseAttributes(viperCfg),
		},
		cfgGRPC: cfgGRPC{
			maxChunkSize:         maxChunkSize,
			maxAddrAmount:        maxAddrAmount,
			enableReflectService: viperCfg.GetBool(cfgReflectService),
		},
		localAddr: netAddr,
		respSvc: response.NewService(
			response.WithNetworkState(state),
		),
		cfgObject: cfgObject{
			pool: initObjectPool(viperCfg),
		},
		netStatus:    atomic.NewInt32(int32(control.NetmapStatus_STATUS_UNDEFINED)),
		healthStatus: atomic.NewInt32(int32(control.HealthStatus_HEALTH_STATUS_UNDEFINED)),
	}

	if viperCfg.GetBool(cfgMetricsEnable) {
		c.metricsCollector = metrics.NewStorageMetrics()
	}

	initLocalStorage(c)

	return c
}

func initViper(path string) *viper.Viper {
	v := viper.New()

	v.SetEnvPrefix(misc.Prefix)
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.SetDefault("app.name", misc.NodeName)
	v.SetDefault("app.version", misc.Version)

	defaultConfiguration(v)

	if path != "" {
		v.SetConfigFile(path)
		v.SetConfigType("yml")
		fatalOnErr(v.ReadInConfig())
	}

	return v
}

func defaultConfiguration(v *viper.Viper) {
	v.SetDefault(cfgNodeKey, "")          // node key
	v.SetDefault(cfgBootstrapAddress, "") // announced address of the node

	v.SetDefault(cfgMorphRPCAddress, []string{})
	v.SetDefault(cfgMorphNotifyRPCAddress, []string{})
	v.SetDefault(cfgMorphNotifyDialTimeout, 5*time.Second)

	v.SetDefault(cfgListenAddress, "127.0.0.1:50501") // listen address
	v.SetDefault(cfgMaxMsgSize, 4<<20)                // transport msg limit 4 MiB
	v.SetDefault(cfgReflectService, false)
	v.SetDefault(cfgDialTimeout, 5*time.Second)

	v.SetDefault(cfgAccountingContract, "1aeefe1d0dfade49740fff779c02cd4a0538ffb1")
	v.SetDefault(cfgAccountingFee, "1")

	v.SetDefault(cfgContainerContract, "9d2ca84d7fb88213c4baced5a6ed4dc402309039")
	v.SetDefault(cfgContainerFee, "1")
	v.SetDefault(cfgContainerWorkerPoolEnabled, true)
	v.SetDefault(cfgContainerWorkerPoolSize, 10)

	v.SetDefault(cfgNetmapContract, "75194459637323ea8837d2afe8225ec74a5658c3")
	v.SetDefault(cfgNetmapFee, "1")
	v.SetDefault(cfgNetmapWorkerPoolEnabled, true)
	v.SetDefault(cfgNetmapWorkerPoolSize, 10)

	v.SetDefault(cfgLogLevel, "info")
	v.SetDefault(cfgLogFormat, "console")
	v.SetDefault(cfgLogTrace, "fatal")
	v.SetDefault(cfgLogInitSampling, 1000)
	v.SetDefault(cfgLogThereafterSampling, 1000)

	v.SetDefault(cfgProfilerEnable, false)
	v.SetDefault(cfgProfilerAddr, ":6060")
	v.SetDefault(cfgProfilerTTL, "30s")

	v.SetDefault(cfgMetricsEnable, false)
	v.SetDefault(cfgMetricsAddr, ":9090")

	v.SetDefault(cfgGCQueueSize, 1000)
	v.SetDefault(cfgGCQueueTick, "5s")
	v.SetDefault(cfgGCTimeout, "5s")

	v.SetDefault(cfgPolicerWorkScope, 100)
	v.SetDefault(cfgPolicerExpRate, 10) // in %
	v.SetDefault(cfgPolicerHeadTimeout, 5*time.Second)

	v.SetDefault(cfgReplicatorPutTimeout, 5*time.Second)

	v.SetDefault(cfgReBootstrapEnabled, false) // in epochs
	v.SetDefault(cfgReBootstrapInterval, 2)    // in epochs

	v.SetDefault(cfgObjectGetPoolSize, 10)
	v.SetDefault(cfgObjectHeadPoolSize, 10)
	v.SetDefault(cfgObjectPutPoolSize, 10)
	v.SetDefault(cfgObjectSearchPoolSize, 10)
	v.SetDefault(cfgObjectRangePoolSize, 10)
	v.SetDefault(cfgObjectRangeHashPoolSize, 10)

	v.SetDefault(cfgCtrlSvcAuthorizedKeys, []string{})

	v.SetDefault(cfgTombstoneLifetime, 5)
}

func (c *cfg) LocalAddress() *network.Address {
	return c.localAddr
}

func initLocalStorage(c *cfg) {
	initShardOptions(c)

	engineOpts := []engine.Option{engine.WithLogger(c.log)}
	if c.metricsCollector != nil {
		engineOpts = append(engineOpts, engine.WithMetrics(c.metricsCollector))
	}

	ls := engine.New(engineOpts...)

	for _, opts := range c.cfgObject.cfgLocalStorage.shardOpts {
		id, err := ls.AddShard(opts...)
		fatalOnErr(err)

		c.log.Info("shard attached to engine",
			zap.Stringer("id", id),
		)
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

func initShardOptions(c *cfg) {
	var opts [][]shard.Option

	for i := 0; ; i++ {
		prefix := configPath(
			cfgLocalStorageSection,
			cfgStorageShardSection,
			strconv.Itoa(i),
		)

		useCache := c.viper.GetBool(
			configPath(prefix, cfgShardUseWriteCache),
		)

		writeCachePrefix := configPath(prefix, cfgWriteCacheSection)

		writeCachePath := c.viper.GetString(
			configPath(writeCachePrefix, cfgBlobStorTreePath),
		)
		if useCache && writeCachePath == "" {
			c.log.Warn("incorrect writeCache path, ignore shard")
			break
		}

		blobPrefix := configPath(prefix, cfgBlobStorSection)

		blobPath := c.viper.GetString(
			configPath(blobPrefix, cfgBlobStorTreePath),
		)
		if blobPath == "" {
			c.log.Warn("incorrect blobStor path, ignore shard")
			break
		}

		compressObjects := c.viper.GetBool(
			configPath(blobPrefix, cfgBlobStorCompress),
		)

		blobPerm := os.FileMode(c.viper.GetInt(
			configPath(blobPrefix, cfgBlobStorTreePerm),
		))

		shallowDepth := c.viper.GetInt(
			configPath(blobPrefix, cfgBlobStorShallowDepth),
		)

		smallSzLimit := c.viper.GetUint64(
			configPath(blobPrefix, cfgBlobStorSmallSzLimit),
		)
		if smallSzLimit == 0 {
			smallSzLimit = 1 << 20 // 1MB
		}

		blzPrefix := configPath(blobPrefix, cfgBlobStorBlzSection)

		blzSize := c.viper.GetUint64(
			configPath(blzPrefix, cfgBlzSize),
		)
		if blzSize == 0 {
			blzSize = 1 << 30 // 1 GB
		}

		blzShallowDepth := c.viper.GetUint64(
			configPath(blzPrefix, cfgBlzShallowDepth),
		)

		blzShallowWidth := c.viper.GetUint64(
			configPath(blzPrefix, cfgBlzShallowWidth),
		)

		blzCacheSize := c.viper.GetInt(
			configPath(blzPrefix, cfgBlzOpenedCacheSize),
		)

		metaPrefix := configPath(prefix, cfgMetaBaseSection)

		metaPath := c.viper.GetString(
			configPath(metaPrefix, cfgMetaBasePath),
		)

		metaPerm := os.FileMode(c.viper.GetUint32(
			configPath(metaPrefix, cfgMetaBasePerm),
		))

		fatalOnErr(os.MkdirAll(path.Dir(metaPath), metaPerm))

		gcPrefix := configPath(prefix, cfgGCSection)

		rmBatchSize := c.viper.GetInt(
			configPath(gcPrefix, cfgGCRemoverBatchSize),
		)

		rmSleepInterval := c.viper.GetDuration(
			configPath(gcPrefix, cfgGCRemoverSleepInt),
		)

		opts = append(opts, []shard.Option{
			shard.WithLogger(c.log),
			shard.WithBlobStorOptions(
				blobstor.WithRootPath(blobPath),
				blobstor.WithCompressObjects(compressObjects, c.log),
				blobstor.WithRootPerm(blobPerm),
				blobstor.WithShallowDepth(shallowDepth),
				blobstor.WithSmallSizeLimit(smallSzLimit),
				blobstor.WithBlobovniczaSize(blzSize),
				blobstor.WithBlobovniczaShallowDepth(blzShallowDepth),
				blobstor.WithBlobovniczaShallowWidth(blzShallowWidth),
				blobstor.WithBlobovniczaOpenedCacheSize(blzCacheSize),
				blobstor.WithLogger(c.log),
			),
			shard.WithMetaBaseOptions(
				meta.WithLogger(c.log),
				meta.WithPath(metaPath),
				meta.WithPermissions(metaPerm),
				meta.WithBoltDBOptions(&bbolt.Options{
					Timeout: 100 * time.Millisecond,
				}),
			),
			shard.WithWriteCache(useCache),
			shard.WithWriteCacheOptions(
				blobstor.WithRootPath(writeCachePath),
				blobstor.WithBlobovniczaShallowDepth(0),
				blobstor.WithBlobovniczaShallowWidth(1),
			),
			shard.WithRemoverBatchSize(rmBatchSize),
			shard.WithGCRemoverSleepInterval(rmSleepInterval),
			shard.WithGCWorkerPoolInitializer(func(sz int) util2.WorkerPool {
				pool, err := ants.NewPool(sz)
				fatalOnErr(err)

				return pool
			}),
			shard.WithGCEventChannelInitializer(func() <-chan shard.Event {
				ch := make(chan shard.Event)

				addNewEpochNotificationHandler(c, func(ev event.Event) {
					ch <- shard.EventNewEpoch(ev.(netmap2.NewEpoch).EpochNumber())
				})

				return ch
			}),
		})

		c.log.Info("storage shard options",
			zap.Bool("with write cache", useCache),
			zap.String("with write cache path", writeCachePath),
			zap.String("BLOB path", blobPath),
			zap.Stringer("BLOB permissions", blobPerm),
			zap.Bool("BLOB compress", compressObjects),
			zap.Int("BLOB shallow depth", shallowDepth),
			zap.Uint64("BLOB small size limit", smallSzLimit),
			zap.String("metabase path", metaPath),
			zap.Stringer("metabase permissions", metaPerm),
			zap.Int("GC remover batch size", rmBatchSize),
			zap.Duration("GC remover sleep interval", rmSleepInterval),
		)
	}

	if len(opts) == 0 {
		fatalOnErr(errors.New("no correctly set up shards, exit"))
	}

	c.cfgObject.cfgLocalStorage.shardOpts = opts
}

func configPath(sections ...string) string {
	return strings.Join(sections, ".")
}

func initObjectPool(cfg *viper.Viper) (pool cfgObjectRoutines) {
	var err error

	optNonBlocking := ants.WithNonblocking(true)

	pool.get, err = ants.NewPool(cfg.GetInt(cfgObjectGetPoolSize), optNonBlocking)
	if err != nil {
		fatalOnErr(err)
	}

	pool.head, err = ants.NewPool(cfg.GetInt(cfgObjectHeadPoolSize), optNonBlocking)
	if err != nil {
		fatalOnErr(err)
	}

	pool.search, err = ants.NewPool(cfg.GetInt(cfgObjectSearchPoolSize), optNonBlocking)
	if err != nil {
		fatalOnErr(err)
	}

	pool.put, err = ants.NewPool(cfg.GetInt(cfgObjectPutPoolSize), optNonBlocking)
	if err != nil {
		fatalOnErr(err)
	}

	pool.rng, err = ants.NewPool(cfg.GetInt(cfgObjectRangePoolSize), optNonBlocking)
	if err != nil {
		fatalOnErr(err)
	}

	pool.rngHash, err = ants.NewPool(cfg.GetInt(cfgObjectRangeHashPoolSize), optNonBlocking)
	if err != nil {
		fatalOnErr(err)
	}

	return pool
}

func initNetmapWorkerPool(v *viper.Viper) (util2.WorkerPool, error) {
	if v.GetBool(cfgNetmapWorkerPoolEnabled) {
		// return async worker pool
		return ants.NewPool(v.GetInt(cfgNetmapWorkerPoolSize))
	}

	// return sync worker pool
	return util2.SyncWorkerPool{}, nil
}

func initContainerWorkerPool(v *viper.Viper) (util2.WorkerPool, error) {
	if v.GetBool(cfgContainerWorkerPoolEnabled) {
		// return async worker pool
		return ants.NewPool(v.GetInt(cfgContainerWorkerPoolSize))
	}

	// return sync worker pool
	return util2.SyncWorkerPool{}, nil
}

func (c *cfg) LocalNodeInfo() (*netmapV2.NodeInfo, error) {
	ni := c.localNodeInfo()
	return ni.ToV2(), nil
}

// handleLocalNodeInfo rewrites local node info
func (c *cfg) handleLocalNodeInfo(ni *netmap.NodeInfo) {
	c.cfgNodeInfo.infoMtx.Lock()

	if ni != nil {
		c.cfgNodeInfo.info = *ni
	}

	c.updateStatusWithoutLock(ni)

	c.cfgNodeInfo.infoMtx.Unlock()
}

// handleNodeInfoStatus updates node info status without rewriting whole local
// node info status
func (c *cfg) handleNodeInfoStatus(ni *netmap.NodeInfo) {
	c.cfgNodeInfo.infoMtx.Lock()

	c.updateStatusWithoutLock(ni)

	c.cfgNodeInfo.infoMtx.Unlock()
}

func (c *cfg) localNodeInfo() netmap.NodeInfo {
	c.cfgNodeInfo.infoMtx.RLock()
	defer c.cfgNodeInfo.infoMtx.RUnlock()

	return c.cfgNodeInfo.info
}

func (c *cfg) toOnlineLocalNodeInfo() *netmap.NodeInfo {
	ni := c.localNodeInfo()
	ni.SetState(netmap.NodeStateOnline)

	return &ni
}

func (c *cfg) updateStatusWithoutLock(ni *netmap.NodeInfo) {
	var nmState netmap.NodeState

	if ni != nil {
		nmState = ni.State()
	} else {
		nmState = netmap.NodeStateOffline
		c.cfgNodeInfo.info.SetState(nmState)
	}

	switch nmState {
	default:
		c.setNetmapStatus(control.NetmapStatus_STATUS_UNDEFINED)
	case netmap.NodeStateOnline:
		c.setNetmapStatus(control.NetmapStatus_ONLINE)
	case netmap.NodeStateOffline:
		c.setNetmapStatus(control.NetmapStatus_OFFLINE)
	}
}
