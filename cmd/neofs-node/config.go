package main

import (
	"context"
	"crypto/ecdsa"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	netmapCore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket/fsbucket"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	nmwrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	tokenStorage "github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-node/pkg/util/profiler"
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

	cfgObjectStorage = "storage.object"

	cfgMetaBasePath = "storage.metabase.path"
	cfgMetaBasePerm = "storage.metabase.perm"

	cfgGCQueueSize = "gc.queuesize"
	cfgGCQueueTick = "gc.duration.sleep"
	cfgGCTimeout   = "gc.duration.timeout"

	cfgPolicerWorkScope   = "policer.work_scope"
	cfgPolicerExpRate     = "policer.expansion_rate"
	cfgPolicerHeadTimeout = "policer.head_timeout"

	cfgReplicatorPutTimeout = "replicator.put_timeout"
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
}

type BootstrapType uint32

type cfgNodeInfo struct {
	// values from config
	bootType   BootstrapType
	attributes []*netmap.Attribute

	// values at runtime
	info *netmap.NodeInfo
}

type cfgObject struct {
	netMapStorage netmapCore.Source

	cnrStorage container.Source

	metastorage *meta.DB

	blobstorage bucket.Bucket

	cnrClient *wrapper.Wrapper
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
			scriptHash: u160Netmap,
			fee:        util.Fixed8(viperCfg.GetInt(cfgNetmapFee)),
			state:      state,
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

	v.SetDefault(cfgObjectStorage+".type", "inmemory")

	v.SetDefault(cfgMetaBasePath, "metabase")
	v.SetDefault(cfgMetaBasePerm, 0600)

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
}

func (c *cfg) LocalAddress() *network.Address {
	return c.localAddr
}

func initLocalStorage(c *cfg) {
	var err error

	c.cfgObject.blobstorage, err = initBucket(cfgObjectStorage, c)
	fatalOnErr(err)

	boltDB, err := bbolt.Open(
		c.viper.GetString(cfgMetaBasePath),
		os.FileMode(c.viper.GetUint32(cfgMetaBasePerm)),
		nil,
	)
	fatalOnErr(err)

	c.cfgObject.metastorage = meta.NewDB(boltDB)
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
