package main

import (
	"context"
	"crypto/ecdsa"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	netmapV2 "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	loggerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/logger"
	metricsconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/metrics"
	"github.com/nspcc-dev/neofs-node/misc"
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
	"github.com/spf13/viper"
	"go.etcd.io/bbolt"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// config keys for cfgNodeInfo
	cfgNodeKey             = "node.key"
	cfgBootstrapAddress    = "node.address"
	cfgNodeAttributePrefix = "node.attribute"
	cfgNodeRelay           = "node.relay"

	// config keys for cfgGRPC
	cfgListenAddress = "grpc.endpoint"
	cfgTLSEnabled    = "grpc.tls.enabled"
	cfgTLSCertFile   = "grpc.tls.certificate"
	cfgTLSKeyFile    = "grpc.tls.key"

	// config keys for API client cache
	cfgAPIClientDialTimeout = "apiclient.dial_timeout"

	// config keys for cfgMorph
	cfgMorphRPCAddress = "morph.rpc_endpoint"

	cfgMorphNotifyRPCAddress  = "morph.notification_endpoint"
	cfgMorphNotifyDialTimeout = "morph.dial_timeout"

	cfgMainChainRPCAddress  = "mainchain.rpc_endpoint"
	cfgMainChainDialTimeout = "mainchain.dial_timeout"

	// config keys for cfgAccounting
	cfgAccountingContract = "accounting.scripthash"

	// config keys for cfgNetmap
	cfgNetmapContract = "netmap.scripthash"

	// config keys for cfgContainer
	cfgContainerContract = "container.scripthash"

	// config keys for cfgReputation
	cfgReputationContract = "reputation.scripthash"

	cfgPolicerHeadTimeout = "policer.head_timeout"

	cfgReplicatorPutTimeout = "replicator.put_timeout"

	cfgObjectPutPoolSize = "object.put.pool_size"
)

const (
	addressSize = 72 // 32 bytes oid, 32 bytes cid, 8 bytes protobuf encoding
)

const maxMsgSize = 4 << 20 // transport msg limit 4 MiB

// capacity of the pools of the morph notification handlers
// for each contract listener.
const notificationHandlerPoolSize = 10

type cfg struct {
	ctx context.Context

	appCfg *config.Config

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

	metricsCollector *metrics.StorageMetrics

	workers []worker

	respSvc *response.Service

	cfgControlService cfgControlService

	netStatus *atomic.Int32

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

const (
	_ BootstrapType = iota
	StorageNode
	RelayNode
)

func initCfg(path string) *cfg {
	var p config.Prm

	appCfg := config.New(p,
		config.WithConfigFile(path),
	)

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

	u160Reputation, err := util.Uint160DecodeStringLE(
		viperCfg.GetString(cfgReputationContract))
	fatalOnErr(err)

	var logPrm logger.Prm

	err = logPrm.SetLevelString(
		loggerconfig.Level(appCfg),
	)
	fatalOnErr(err)

	log, err := logger.NewLogger(logPrm)
	fatalOnErr(err)

	netAddr, err := network.AddressFromString(viperCfg.GetString(cfgBootstrapAddress))
	fatalOnErr(err)

	maxChunkSize := uint64(maxMsgSize) * 3 / 4          // 25% to meta, 75% to payload
	maxAddrAmount := uint64(maxChunkSize) / addressSize // each address is about 72 bytes

	var (
		tlsEnabled  bool
		tlsCertFile string
		tlsKeyFile  string
	)

	if viperCfg.GetBool(cfgTLSEnabled) {
		tlsEnabled = true
		tlsCertFile = viperCfg.GetString(cfgTLSCertFile)
		tlsKeyFile = viperCfg.GetString(cfgTLSKeyFile)
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

	relayOnly := viperCfg.GetBool(cfgNodeRelay)

	c := &cfg{
		ctx:         context.Background(),
		appCfg:      appCfg,
		internalErr: make(chan error),
		viper:       viperCfg,
		log:         log,
		wg:          new(sync.WaitGroup),
		key:         key,
		apiVersion:  pkg.SDKVersion(),
		cfgAccounting: cfgAccounting{
			scriptHash: u160Accounting,
		},
		cfgContainer: cfgContainer{
			scriptHash: u160Container,
			workerPool: containerWorkerPool,
		},
		cfgNetmap: cfgNetmap{
			scriptHash:          u160Netmap,
			state:               state,
			workerPool:          netmapWorkerPool,
			reBootstrapEnabled:  !relayOnly,
			reBoostrapTurnedOff: atomic.NewBool(relayOnly),
		},
		cfgNodeInfo: cfgNodeInfo{
			bootType:   StorageNode,
			attributes: parseAttributes(viperCfg),
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
			pool: initObjectPool(viperCfg),
		},
		netStatus:    atomic.NewInt32(int32(control.NetmapStatus_STATUS_UNDEFINED)),
		healthStatus: atomic.NewInt32(int32(control.HealthStatus_HEALTH_STATUS_UNDEFINED)),
		cfgReputation: cfgReputation{
			scriptHash: u160Reputation,
			workerPool: reputationWorkerPool,
		},
	}

	if metricsconfig.Address(c.appCfg) != "" {
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
	v.SetDefault(cfgNodeRelay, false)

	v.SetDefault(cfgMorphRPCAddress, []string{})
	v.SetDefault(cfgMorphNotifyRPCAddress, []string{})
	v.SetDefault(cfgMorphNotifyDialTimeout, 5*time.Second)

	v.SetDefault(cfgListenAddress, "127.0.0.1:50501") // listen address
	v.SetDefault(cfgTLSEnabled, false)
	v.SetDefault(cfgTLSCertFile, "")
	v.SetDefault(cfgTLSKeyFile, "")

	v.SetDefault(cfgAPIClientDialTimeout, 5*time.Second)

	v.SetDefault(cfgAccountingContract, "")

	v.SetDefault(cfgContainerContract, "")

	v.SetDefault(cfgNetmapContract, "")

	v.SetDefault(cfgPolicerHeadTimeout, 5*time.Second)

	v.SetDefault(cfgReplicatorPutTimeout, 5*time.Second)

	v.SetDefault(cfgObjectPutPoolSize, 10)

	v.SetDefault(cfgCtrlSvcAuthorizedKeys, []string{})
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

func initObjectPool(cfg *viper.Viper) (pool cfgObjectRoutines) {
	var err error

	optNonBlocking := ants.WithNonblocking(true)

	pool.put, err = ants.NewPool(cfg.GetInt(cfgObjectPutPoolSize), optNonBlocking)
	if err != nil {
		fatalOnErr(err)
	}

	return pool
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
