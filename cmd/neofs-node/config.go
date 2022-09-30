package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	atomicstd "sync/atomic"
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
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/metrics"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
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

type cfg struct {
	ctx context.Context

	appCfg *config.Config

	ctxCancel func()

	internalErr chan error // channel for internal application errors at runtime

	log *zap.Logger

	wg *sync.WaitGroup

	key *keys.PrivateKey

	binPublicKey []byte

	ownerIDFromKey user.ID // user ID calculated from key

	apiVersion version.Version

	cfgGRPC cfgGRPC

	cfgMorph cfgMorph

	cfgAccounting cfgAccounting

	cfgContainer cfgContainer

	cfgNetmap cfgNetmap

	privateTokenStore sessionStorage

	cfgNodeInfo cfgNodeInfo

	localAddr network.AddressGroup

	cfgObject cfgObject

	cfgNotifications cfgNotifications

	metricsCollector *metrics.NodeMetrics

	workers []worker

	respSvc *response.Service

	replicator *replicator.Replicator

	cfgControlService cfgControlService

	treeService *tree.Service

	healthStatus *atomic.Int32

	closers []func()

	cfgReputation cfgReputation

	clientCache *cache.ClientCache

	persistate *state.PersistentStorage

	netMapSource netmapCore.Source

	// current network map
	netMap atomicstd.Value // type netmap.NetMap
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

	notaryEnabled bool

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
}

type cfgNotifications struct {
	enabled      bool
	nw           notificationWriter
	defaultTopic string
}

type cfgLocalStorage struct {
	localStorage *engine.StorageEngine

	shardOpts [][]shard.Option
}

type cfgObjectRoutines struct {
	putRemote *ants.Pool

	putRemoteCapacity int

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
	key := nodeconfig.Key(appCfg)

	var logPrm logger.Prm

	err := logPrm.SetLevelString(
		loggerconfig.Level(appCfg),
	)
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

	c := &cfg{
		ctx:          context.Background(),
		appCfg:       appCfg,
		internalErr:  make(chan error),
		log:          log,
		wg:           new(sync.WaitGroup),
		key:          key,
		binPublicKey: key.PublicKey().Bytes(),
		apiVersion:   version.Current(),
		cfgAccounting: cfgAccounting{
			scriptHash: contractsconfig.Balance(appCfg),
		},
		cfgContainer: cfgContainer{
			scriptHash: contractsconfig.Container(appCfg),
			workerPool: containerWorkerPool,
		},
		cfgNetmap: cfgNetmap{
			scriptHash:          contractsconfig.Netmap(appCfg),
			state:               netState,
			workerPool:          netmapWorkerPool,
			needBootstrap:       !relayOnly,
			reBoostrapTurnedOff: atomic.NewBool(relayOnly),
		},
		cfgGRPC: cfgGRPC{
			maxChunkSize:  maxChunkSize,
			maxAddrAmount: maxAddrAmount,
		},
		cfgMorph: cfgMorph{
			proxyScriptHash: contractsconfig.Proxy(appCfg),
		},
		localAddr: netAddr,
		respSvc: response.NewService(
			response.WithNetworkState(netState),
		),
		cfgObject: cfgObject{
			pool: initObjectPool(appCfg),
		},
		healthStatus: atomic.NewInt32(int32(control.HealthStatus_HEALTH_STATUS_UNDEFINED)),
		cfgReputation: cfgReputation{
			scriptHash: contractsconfig.Reputation(appCfg),
			workerPool: reputationWorkerPool,
		},
		clientCache: cache.NewSDKClientCache(cache.ClientCacheOpts{
			DialTimeout:   apiclientconfig.DialTimeout(appCfg),
			StreamTimeout: apiclientconfig.StreamTimeout(appCfg),
			Key:           &key.PrivateKey,
		}),
		persistate: persistate,
	}

	user.IDFromKey(&c.ownerIDFromKey, key.PrivateKey.PublicKey)

	if metricsconfig.Enabled(c.appCfg) {
		c.metricsCollector = metrics.NewNodeMetrics()
		netState.metrics = c.metricsCollector
	}

	c.onShutdown(c.clientCache.CloseAll) // clean up connections
	c.onShutdown(func() { _ = c.persistate.Close() })

	return c
}

func (c *cfg) LocalAddress() network.AddressGroup {
	return c.localAddr
}

func initLocalStorage(c *cfg) {
	initShardOptions(c)

	engineOpts := []engine.Option{
		engine.WithLogger(c.log),
		engine.WithShardPoolSize(engineconfig.ShardPoolSize(c.appCfg)),
		engine.WithErrorThreshold(engineconfig.ShardErrorThreshold(c.appCfg)),
	}
	if c.metricsCollector != nil {
		engineOpts = append(engineOpts, engine.WithMetrics(c.metricsCollector))
	}

	ls := engine.New(engineOpts...)

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

	for _, opts := range c.cfgObject.cfgLocalStorage.shardOpts {
		id, err := ls.AddShard(append(opts, shard.WithTombstoneSource(tombstoneSource))...)
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

	require := !nodeconfig.Relay(c.appCfg) // relay node does not require shards

	err := engineconfig.IterateShards(c.appCfg, require, func(sc *shardconfig.Config) error {
		var writeCacheOpts []writecache.Option

		writeCacheCfg := sc.WriteCache()
		if writeCacheCfg.Enabled() {
			writeCacheOpts = []writecache.Option{
				writecache.WithPath(writeCacheCfg.Path()),
				writecache.WithLogger(c.log),
				writecache.WithMaxBatchSize(writeCacheCfg.BoltDB().MaxBatchSize()),
				writecache.WithMaxBatchDelay(writeCacheCfg.BoltDB().MaxBatchDelay()),
				writecache.WithMaxObjectSize(writeCacheCfg.MaxObjectSize()),
				writecache.WithSmallObjectSize(writeCacheCfg.SmallObjectSize()),
				writecache.WithFlushWorkersCount(writeCacheCfg.WorkersNumber()),
				writecache.WithMaxCacheSize(writeCacheCfg.SizeLimit()),
			}
		}

		blobStorCfg := sc.BlobStor()
		storages := blobStorCfg.Storages()
		metabaseCfg := sc.Metabase()
		gcCfg := sc.GC()

		var piloramaOpts []pilorama.Option

		piloramaCfg := sc.Pilorama()
		if config.BoolSafe(c.appCfg.Sub("tree"), "enabled") {
			piloramaOpts = []pilorama.Option{
				pilorama.WithPath(piloramaCfg.Path()),
				pilorama.WithPerm(piloramaCfg.Perm()),
				pilorama.WithNoSync(piloramaCfg.NoSync()),
				pilorama.WithMaxBatchSize(piloramaCfg.MaxBatchSize()),
				pilorama.WithMaxBatchDelay(piloramaCfg.MaxBatchDelay())}
		}

		var st []blobstor.SubStorage
		for i := range storages {
			switch storages[i].Type() {
			case blobovniczatree.Type:
				sub := blobovniczaconfig.From((*config.Config)(storages[i]))
				lim := sc.SmallSizeLimit()
				st = append(st, blobstor.SubStorage{
					Storage: blobovniczatree.NewBlobovniczaTree(
						blobovniczatree.WithLogger(c.log),
						blobovniczatree.WithRootPath(storages[i].Path()),
						blobovniczatree.WithPermissions(storages[i].Perm()),
						blobovniczatree.WithBlobovniczaSize(sub.Size()),
						blobovniczatree.WithBlobovniczaShallowDepth(sub.ShallowDepth()),
						blobovniczatree.WithBlobovniczaShallowWidth(sub.ShallowWidth()),
						blobovniczatree.WithOpenedCacheSize(sub.OpenedCacheSize())),
					Policy: func(_ *objectSDK.Object, data []byte) bool {
						return uint64(len(data)) < lim
					},
				})
			case fstree.Type:
				sub := fstreeconfig.From((*config.Config)(storages[i]))
				st = append(st, blobstor.SubStorage{
					Storage: fstree.New(
						fstree.WithPath(storages[i].Path()),
						fstree.WithPerm(storages[i].Perm()),
						fstree.WithDepth(sub.Depth())),
					Policy: func(_ *objectSDK.Object, data []byte) bool {
						return true
					},
				})
			default:
				return fmt.Errorf("invalid storage type: %s", storages[i].Type())
			}
		}

		metaPath := metabaseCfg.Path()
		metaPerm := metabaseCfg.BoltDB().Perm()

		opts = append(opts, []shard.Option{
			shard.WithLogger(c.log),
			shard.WithRefillMetabase(sc.RefillMetabase()),
			shard.WithMode(sc.Mode()),
			shard.WithBlobStorOptions(
				blobstor.WithCompressObjects(sc.Compress()),
				blobstor.WithUncompressableContentTypes(sc.UncompressableContentTypes()),
				blobstor.WithStorages(st),
				blobstor.WithLogger(c.log),
			),
			shard.WithMetaBaseOptions(
				meta.WithLogger(c.log),
				meta.WithPath(metaPath),
				meta.WithPermissions(metaPerm),
				meta.WithMaxBatchSize(metabaseCfg.BoltDB().MaxBatchSize()),
				meta.WithMaxBatchDelay(metabaseCfg.BoltDB().MaxBatchDelay()),
				meta.WithBoltDBOptions(&bbolt.Options{
					Timeout: 100 * time.Millisecond,
				}),
				meta.WithEpochState(c.cfgNetmap.state),
			),
			shard.WithPiloramaOptions(piloramaOpts...),
			shard.WithWriteCache(writeCacheCfg.Enabled()),
			shard.WithWriteCacheOptions(writeCacheOpts...),
			shard.WithRemoverBatchSize(gcCfg.RemoverBatchSize()),
			shard.WithGCRemoverSleepInterval(gcCfg.RemoverSleepInterval()),
			shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
				pool, err := ants.NewPool(sz)
				fatalOnErr(err)

				return pool
			}),
		})
		return nil
	})
	if err != nil {
		panic(err)
	}

	c.cfgObject.cfgLocalStorage.shardOpts = opts
}

func initObjectPool(cfg *config.Config) (pool cfgObjectRoutines) {
	var err error

	optNonBlocking := ants.WithNonblocking(true)

	pool.putRemoteCapacity = objectconfig.Put(cfg).PoolSizeRemote()

	pool.putRemote, err = ants.NewPool(pool.putRemoteCapacity, optNonBlocking)
	fatalOnErr(err)

	pool.replication, err = ants.NewPool(pool.putRemoteCapacity)
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
}

// bootstrap sets local node's netmap status to "online".
func (c *cfg) bootstrap() error {
	ni := c.cfgNodeInfo.localInfo
	ni.SetOnline()

	prm := nmClient.AddPeerPrm{}
	prm.SetNodeInfo(ni)

	return c.cfgNetmap.wrapper.AddPeer(prm)
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
