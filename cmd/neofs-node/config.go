package main

import (
	"context"
	"net"
	"os"
	"path"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	netmapV2 "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	contractsconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/contracts"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	loggerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/logger"
	metricsconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/metrics"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	objectconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/metrics"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cntwrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	nmwrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmap2 "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/timer"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	tokenStorage "github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
	util2 "github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/panjf2000/ants/v2"
	"go.etcd.io/bbolt"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const addressSize = 72 // 32 bytes oid, 32 bytes cid, 8 bytes protobuf encoding

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

	metricsCollector *metrics.StorageMetrics

	workers []worker

	respSvc *response.Service

	cfgControlService cfgControlService

	healthStatus *atomic.Int32

	closers []func()

	cfgReputation cfgReputation

	mainChainClient *client.Client
}

type cfgGRPC struct {
	listener net.Listener

	server *grpc.Server

	maxChunkSize uint64

	maxAddrAmount uint64

	tlsEnabled  bool
	tlsCertFile string
	tlsKeyFile  string
}

type cfgMorph struct {
	client *client.Client

	blockTimers     []*timer.BlockTimer // all combined timers
	eigenTrustTimer *timer.BlockTimer   // timer for EigenTrust iterations
}

type cfgAccounting struct {
	scriptHash util.Uint160
}

type cfgContainer struct {
	scriptHash util.Uint160

	parsers     map[event.Type]event.Parser
	subscribers map[event.Type][]event.Handler
	workerPool  util2.WorkerPool // pool for asynchronous handlers
}

type cfgNetmap struct {
	scriptHash util.Uint160
	wrapper    *nmwrapper.Wrapper

	parsers map[event.Type]event.Parser

	subscribers map[event.Type][]event.Handler
	workerPool  util2.WorkerPool // pool for asynchronous handlers

	state *networkState

	reBootstrapEnabled  bool
	reBoostrapTurnedOff *atomic.Bool // managed by control service in runtime
	startEpoch          uint64       // epoch number when application is started
}

type cfgNodeInfo struct {
	// values from config
	localInfo netmap.NodeInfo
}

type cfgObject struct {
	netMapStorage netmapCore.Source

	cnrStorage container.Source

	cnrClient *cntwrapper.Wrapper

	pool cfgObjectRoutines

	cfgLocalStorage cfgLocalStorage
}

type cfgLocalStorage struct {
	localStorage *engine.StorageEngine

	shardOpts [][]shard.Option
}

type cfgObjectRoutines struct {
	put *ants.Pool
}

type cfgControlService struct {
	server *grpc.Server
}

type cfgReputation struct {
	workerPool util2.WorkerPool // pool for EigenTrust algorithm's iterations

	localTrustStorage *truststorage.Storage

	localTrustCtrl *trustcontroller.Controller

	scriptHash util.Uint160
}

func initCfg(path string) *cfg {
	var p config.Prm

	appCfg := config.New(p,
		config.WithConfigFile(path),
	)

	key := nodeconfig.Key(appCfg)

	var logPrm logger.Prm

	err := logPrm.SetLevelString(
		loggerconfig.Level(appCfg),
	)
	fatalOnErr(err)

	log, err := logger.NewLogger(logPrm)
	fatalOnErr(err)

	netAddr := nodeconfig.BootstrapAddress(appCfg)

	maxChunkSize := uint64(maxMsgSize) * 3 / 4          // 25% to meta, 75% to payload
	maxAddrAmount := uint64(maxChunkSize) / addressSize // each address is about 72 bytes

	var (
		tlsEnabled  bool
		tlsCertFile string
		tlsKeyFile  string

		tlsConfig = grpcconfig.TLS(appCfg)
	)

	if tlsConfig.Enabled() {
		tlsEnabled = true
		tlsCertFile = tlsConfig.CertificateFile()
		tlsKeyFile = tlsConfig.KeyFile()
	}

	if tlsEnabled {
		netAddr.AddTLS()
	}

	state := newNetworkState()

	containerWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	netmapWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	reputationWorkerPool, err := ants.NewPool(notificationHandlerPoolSize)
	fatalOnErr(err)

	relayOnly := nodeconfig.Relay(appCfg)

	c := &cfg{
		ctx:         context.Background(),
		appCfg:      appCfg,
		internalErr: make(chan error),
		log:         log,
		wg:          new(sync.WaitGroup),
		key:         key,
		apiVersion:  pkg.SDKVersion(),
		cfgAccounting: cfgAccounting{
			scriptHash: contractsconfig.Balance(appCfg),
		},
		cfgContainer: cfgContainer{
			scriptHash: contractsconfig.Container(appCfg),
			workerPool: containerWorkerPool,
		},
		cfgNetmap: cfgNetmap{
			scriptHash:          contractsconfig.Netmap(appCfg),
			state:               state,
			workerPool:          netmapWorkerPool,
			reBootstrapEnabled:  !relayOnly,
			reBoostrapTurnedOff: atomic.NewBool(relayOnly),
		},
		cfgGRPC: cfgGRPC{
			maxChunkSize:  maxChunkSize,
			maxAddrAmount: maxAddrAmount,
			tlsEnabled:    tlsEnabled,
			tlsCertFile:   tlsCertFile,
			tlsKeyFile:    tlsKeyFile,
		},
		localAddr: netAddr,
		respSvc: response.NewService(
			response.WithNetworkState(state),
		),
		cfgObject: cfgObject{
			pool: initObjectPool(appCfg),
		},
		healthStatus: atomic.NewInt32(int32(control.HealthStatus_HEALTH_STATUS_UNDEFINED)),
		cfgReputation: cfgReputation{
			scriptHash: contractsconfig.Reputation(appCfg),
			workerPool: reputationWorkerPool,
		},
	}

	if metricsconfig.Address(c.appCfg) != "" {
		c.metricsCollector = metrics.NewStorageMetrics()
	}

	initLocalStorage(c)

	return c
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

	engineconfig.IterateShards(c.appCfg, func(sc *shardconfig.Config) {
		var writeCacheOpts []writecache.Option

		useWriteCache := sc.UseWriteCache()
		if useWriteCache {
			writeCacheCfg := sc.WriteCache()

			writeCacheOpts = []writecache.Option{
				writecache.WithPath(writeCacheCfg.Path()),
				writecache.WithLogger(c.log),
				writecache.WithMaxMemSize(writeCacheCfg.MemSize()),
				writecache.WithMaxObjectSize(writeCacheCfg.MaxObjectSize()),
				writecache.WithSmallObjectSize(writeCacheCfg.SmallObjectSize()),
				writecache.WithMaxDBSize(writeCacheCfg.MaxDBSize()),
				writecache.WithFlushWorkersCount(writeCacheCfg.WorkersNumber()),
			}
		}

		blobStorCfg := sc.BlobStor()
		blobovniczaCfg := blobStorCfg.Blobovnicza()
		metabaseCfg := sc.Metabase()
		gcCfg := sc.GC()

		metaPath := metabaseCfg.Path()
		metaPerm := metabaseCfg.Perm()
		fatalOnErr(os.MkdirAll(path.Dir(metaPath), metaPerm))

		opts = append(opts, []shard.Option{
			shard.WithLogger(c.log),
			shard.WithBlobStorOptions(
				blobstor.WithRootPath(blobStorCfg.Path()),
				blobstor.WithCompressObjects(blobStorCfg.Compress(), c.log),
				blobstor.WithRootPerm(blobStorCfg.Perm()),
				blobstor.WithShallowDepth(blobStorCfg.ShallowDepth()),
				blobstor.WithSmallSizeLimit(blobStorCfg.SmallSizeLimit()),
				blobstor.WithBlobovniczaSize(blobovniczaCfg.Size()),
				blobstor.WithBlobovniczaShallowDepth(blobovniczaCfg.ShallowDepth()),
				blobstor.WithBlobovniczaShallowWidth(blobovniczaCfg.ShallowWidth()),
				blobstor.WithBlobovniczaOpenedCacheSize(blobovniczaCfg.OpenedCacheSize()),
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
			shard.WithWriteCache(useWriteCache),
			shard.WithWriteCacheOptions(writeCacheOpts...),
			shard.WithRemoverBatchSize(gcCfg.RemoverBatchSize()),
			shard.WithGCRemoverSleepInterval(gcCfg.RemoverSleepInterval()),
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
	})

	c.cfgObject.cfgLocalStorage.shardOpts = opts
}

func initObjectPool(cfg *config.Config) (pool cfgObjectRoutines) {
	var err error

	optNonBlocking := ants.WithNonblocking(true)

	pool.put, err = ants.NewPool(objectconfig.Put(cfg).PoolSize(), optNonBlocking)
	if err != nil {
		fatalOnErr(err)
	}

	return pool
}

func (c *cfg) LocalNodeInfo() (*netmapV2.NodeInfo, error) {
	ni := c.cfgNetmap.state.getNodeInfo()
	if ni != nil {
		return ni.ToV2(), nil
	}

	return c.cfgNodeInfo.localInfo.ToV2(), nil
}

// handleLocalNodeInfo rewrites local node info from netmap
func (c *cfg) handleLocalNodeInfo(ni *netmap.NodeInfo) {
	c.cfgNetmap.state.setNodeInfo(ni)
}

func (c *cfg) bootstrap() error {
	ni := c.cfgNodeInfo.localInfo
	ni.SetState(netmap.NodeStateOnline)

	return c.cfgNetmap.wrapper.AddPeer(&ni)
}
