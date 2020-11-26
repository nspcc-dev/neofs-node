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

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket/fsbucket"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	nmwrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	tokenStorage "github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-node/pkg/util/profiler"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.etcd.io/bbolt"
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

	// config keys for cfgNodeInfo
	cfgNodeKey             = "node.key"
	cfgBootstrapAddress    = "node.address"
	cfgNodeAttributePrefix = "node.attribute"

	// config keys for cfgGRPC
	cfgListenAddress  = "grpc.endpoint"
	cfgMaxMsgSize     = "grpc.maxmessagesize"
	cfgReflectService = "grpc.enable_reflect_service"

	// config keys for cfgMorph
	cfgMorphRPCAddress = "morph.endpoint"

	cfgMorphNotifyRPCAddress  = "morph.notification.endpoint"
	cfgMorphNotifyDialTimeout = "morph.notification.dial_timeout"

	// config keys for cfgAccounting
	cfgAccountingContract = "accounting.scripthash"
	cfgAccountingFee      = "accounting.fee"

	// config keys for cfgNetmap
	cfgNetmapContract = "netmap.scripthash"
	cfgNetmapFee      = "netmap.fee"

	// config keys for cfgContainer
	cfgContainerContract = "container.scripthash"
	cfgContainerFee      = "container.fee"

	cfgGCQueueSize = "gc.queuesize"
	cfgGCQueueTick = "gc.duration.sleep"
	cfgGCTimeout   = "gc.duration.timeout"

	cfgPolicerWorkScope   = "policer.work_scope"
	cfgPolicerExpRate     = "policer.expansion_rate"
	cfgPolicerHeadTimeout = "policer.head_timeout"
	cfgPolicerDialTimeout = "policer.dial_timeout"

	cfgReplicatorPutTimeout  = "replicator.put_timeout"
	cfgReplicatorDialTimeout = "replicator.dial_timeout"

	cfgReBootstrapEnabled  = "bootstrap.periodic.enabled"
	cfgReBootstrapInterval = "bootstrap.periodic.interval"

	cfgObjectPutPoolSize       = "pool.object.put.size"
	cfgObjectGetPoolSize       = "pool.object.get.size"
	cfgObjectHeadPoolSize      = "pool.object.head.size"
	cfgObjectSearchPoolSize    = "pool.object.search.size"
	cfgObjectRangePoolSize     = "pool.object.range.size"
	cfgObjectRangeHashPoolSize = "pool.object.rangehash.size"

	cfgObjectPutDialTimeout       = "object.put.dial_timeout"
	cfgObjectHeadDialTimeout      = "object.head.dial_timeout"
	cfgObjectRangeDialTimeout     = "object.range.dial_timeout"
	cfgObjectRangeHashDialTimeout = "object.rangehash.dial_timeout"
	cfgObjectSearchDialTimeout    = "object.search.dial_timeout"
)

const (
	cfgLocalStorageSection = "storage"
	cfgStorageShardSection = "shard"

	cfgBlobStorSection      = "blobstor"
	cfgBlobStorCompress     = "compress"
	cfgBlobStorShallowDepth = "shallow_depth"
	cfgBlobStorTreePath     = "path"
	cfgBlobStorTreePerm     = "perm"
	cfgBlobStorSmallSzLimit = "small_size_limit"

	cfgMetaBaseSection = "metabase"
	cfgMetaBasePath    = "path"
	cfgMetaBasePerm    = "perm"
)

const (
	addressSize = 72 // 32 bytes oid, 32 bytes cid, 8 bytes protobuf encoding
)

type cfg struct {
	ctx context.Context

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

	workers []worker

	respSvc *response.Service
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

	fee util.Fixed8
}

type cfgContainer struct {
	scriptHash util.Uint160

	fee util.Fixed8
}

type cfgNetmap struct {
	scriptHash util.Uint160
	wrapper    *nmwrapper.Wrapper

	fee util.Fixed8

	parsers map[event.Type]event.Parser

	subscribers map[event.Type][]event.Handler

	state *networkState

	reBootstrapEnabled  bool
	reBootstrapInterval uint64 // in epochs
}

type BootstrapType uint32

type cfgNodeInfo struct {
	// values from config
	bootType   BootstrapType
	attributes []*netmap.NodeAttribute

	// values at runtime
	info *netmap.NodeInfo
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

	c := &cfg{
		ctx:        context.Background(),
		viper:      viperCfg,
		log:        log,
		wg:         new(sync.WaitGroup),
		key:        key,
		apiVersion: pkg.SDKVersion(),
		cfgAccounting: cfgAccounting{
			scriptHash: u160Accounting,
			fee:        util.Fixed8(viperCfg.GetInt(cfgAccountingFee)),
		},
		cfgContainer: cfgContainer{
			scriptHash: u160Container,
			fee:        util.Fixed8(viperCfg.GetInt(cfgContainerFee)),
		},
		cfgNetmap: cfgNetmap{
			scriptHash:          u160Netmap,
			fee:                 util.Fixed8(viperCfg.GetInt(cfgNetmapFee)),
			state:               state,
			reBootstrapInterval: viperCfg.GetUint64(cfgReBootstrapInterval),
			reBootstrapEnabled:  viperCfg.GetBool(cfgReBootstrapEnabled),
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
	// fixme: all hardcoded private keys must be removed
	v.SetDefault(cfgNodeKey, "Kwk6k2eC3L3QuPvD8aiaNyoSXgQ2YL1bwS5CP1oKoA9waeAze97s")
	v.SetDefault(cfgBootstrapAddress, "") // address to bootstrap with

	v.SetDefault(cfgMorphRPCAddress, "http://morph_chain.localtest.nspcc.ru:30333/")
	v.SetDefault(cfgMorphNotifyRPCAddress, "ws://morph_chain:30333/ws")
	v.SetDefault(cfgMorphNotifyDialTimeout, 5*time.Second)
	v.SetDefault(cfgListenAddress, "127.0.0.1:50501") // listen address
	v.SetDefault(cfgMaxMsgSize, 4<<20)                // transport msg limit 4 MiB

	v.SetDefault(cfgAccountingContract, "1aeefe1d0dfade49740fff779c02cd4a0538ffb1")
	v.SetDefault(cfgAccountingFee, "1")

	v.SetDefault(cfgContainerContract, "9d2ca84d7fb88213c4baced5a6ed4dc402309039")
	v.SetDefault(cfgContainerFee, "1")

	v.SetDefault(cfgNetmapContract, "75194459637323ea8837d2afe8225ec74a5658c3")
	v.SetDefault(cfgNetmapFee, "1")

	v.SetDefault(cfgLogLevel, "info")
	v.SetDefault(cfgLogFormat, "console")
	v.SetDefault(cfgLogTrace, "fatal")
	v.SetDefault(cfgLogInitSampling, 1000)
	v.SetDefault(cfgLogThereafterSampling, 1000)

	v.SetDefault(cfgProfilerEnable, false)
	v.SetDefault(cfgProfilerAddr, ":6060")
	v.SetDefault(cfgProfilerTTL, "30s")

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
}

func (c *cfg) LocalAddress() *network.Address {
	return c.localAddr
}

func initLocalStorage(c *cfg) {
	initShardOptions(c)

	ls := engine.New(
		engine.WithLogger(c.log),
	)

	for _, opts := range c.cfgObject.cfgLocalStorage.shardOpts {
		id, err := ls.AddShard(opts...)
		fatalOnErr(err)

		c.log.Info("shard attached to engine",
			zap.Stringer("id", id),
		)
	}

	c.cfgObject.cfgLocalStorage.localStorage = ls
}

func initShardOptions(c *cfg) {
	var opts [][]shard.Option

	for i := 0; ; i++ {
		prefix := configPath(
			cfgLocalStorageSection,
			cfgStorageShardSection,
			strconv.Itoa(i),
		)

		blobPrefix := configPath(prefix, cfgBlobStorSection)

		blobPath := c.viper.GetString(
			configPath(blobPrefix, cfgBlobStorTreePath),
		)
		if blobPath == "" {
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

		metaPrefix := configPath(prefix, cfgMetaBaseSection)

		metaPath := c.viper.GetString(
			configPath(metaPrefix, cfgMetaBasePath),
		)

		metaPerm := os.FileMode(c.viper.GetUint32(
			configPath(metaPrefix, cfgMetaBasePerm),
		))

		fatalOnErr(os.MkdirAll(path.Dir(metaPath), metaPerm))

		boltDB, err := bbolt.Open(metaPath, metaPerm, nil)
		fatalOnErr(err)

		opts = append(opts, []shard.Option{
			shard.WithLogger(c.log),
			shard.WithBlobStorOptions(
				blobstor.WithTreeRootPath(blobPath),
				blobstor.WithCompressObjects(compressObjects, c.log),
				blobstor.WithTreeRootPerm(blobPerm),
				blobstor.WithShallowDepth(shallowDepth),
				blobstor.WithSmallSizeLimit(smallSzLimit),
			),
			shard.WithMetaBaseOptions(
				meta.WithLogger(c.log),
				meta.FromBoltDB(boltDB),
			),
		})

		c.log.Info("storage shard options",
			zap.String("BLOB path", blobPath),
			zap.Stringer("BLOB permissions", blobPerm),
			zap.Bool("BLOB compress", compressObjects),
			zap.Int("BLOB shallow depth", shallowDepth),
			zap.Uint64("BLOB small size limit", smallSzLimit),
			zap.String("metabase path", metaPath),
			zap.Stringer("metabase permissions", metaPerm),
		)
	}

	c.cfgObject.cfgLocalStorage.shardOpts = opts
}

func configPath(sections ...string) string {
	return strings.Join(sections, ".")
}

func initBucket(prefix string, c *cfg) (bucket bucket.Bucket, err error) {
	const inmemory = "inmemory"

	switch c.viper.GetString(prefix + ".type") {
	case inmemory:
		bucket = newBucket()
		c.log.Info("using in-memory bucket", zap.String("storage", prefix))
	case fsbucket.Name:
		bucket, err = fsbucket.NewBucket(prefix, c.viper)
		if err != nil {
			return nil, errors.Wrap(err, "can't create fs bucket")
		}
		c.log.Info("using filesystem bucket", zap.String("storage", prefix))
	default:
		return nil, errors.New("unknown storage type")
	}

	return bucket, nil
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
