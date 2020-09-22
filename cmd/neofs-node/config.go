package main

import (
	"context"
	"crypto/ecdsa"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/util"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	tokenStorage "github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	// config keys for cfgNodeInfo
	cfgNodeKey             = "node.key"
	cfgBootstrapAddress    = "node.address"
	cfgNodeAttributePrefix = "node.attribute"

	// config keys for cfgGRPC
	cfgListenAddress = "grpc.endpoint"

	// config keys for cfgMorph
	cfgMorphRPCAddress = "morph.endpoint"

	// config keys for cfgAccounting
	cfgAccountingContract = "accounting.scripthash"
	cfgAccountingFee      = "accounting.fee"

	// config keys for cfgNetmap
	cfgNetmapContract = "netmap.scripthash"
	cfgNetmapFee      = "netmap.fee"

	// config keys for cfgContainer
	cfgContainerContract = "container.scripthash"
	cfgContainerFee      = "container.fee"
)

type cfg struct {
	ctx context.Context

	viper *viper.Viper

	wg *sync.WaitGroup

	key *ecdsa.PrivateKey

	cfgGRPC cfgGRPC

	cfgMorph cfgMorph

	cfgAccounting cfgAccounting

	cfgContainer cfgContainer

	cfgNetmap cfgNetmap

	privateTokenStore *tokenStorage.TokenStore

	cfgNodeInfo cfgNodeInfo
}

type cfgGRPC struct {
	listener net.Listener

	server *grpc.Server
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

	fee util.Fixed8
}

type BootstrapType uint32

type cfgNodeInfo struct {
	bootType   BootstrapType
	attributes []string
	capacity   uint64 // default: 0
	price      uint64 // default: 0
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

	return &cfg{
		ctx:   context.Background(),
		viper: viperCfg,
		wg:    new(sync.WaitGroup),
		key:   key,
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
		},
		cfgNodeInfo: cfgNodeInfo{
			bootType:   StorageNode,
			attributes: readAttributes(viperCfg),
		},
	}
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
	v.SetDefault(cfgListenAddress, "127.0.0.1:50501") // listen address

	v.SetDefault(cfgAccountingContract, "1aeefe1d0dfade49740fff779c02cd4a0538ffb1")
	v.SetDefault(cfgAccountingFee, "1")

	v.SetDefault(cfgContainerContract, "9d2ca84d7fb88213c4baced5a6ed4dc402309039")
	v.SetDefault(cfgContainerFee, "1")

	v.SetDefault(cfgNetmapContract, "75194459637323ea8837d2afe8225ec74a5658c3")
	v.SetDefault(cfgNetmapFee, "1")
}

func readAttributes(v *viper.Viper) (attrs []string) {
	const maxAttributes = 100

	for i := 0; i < maxAttributes; i++ {
		attr := v.GetString(cfgNodeAttributePrefix + "_" + strconv.Itoa(i))
		if attr == "" {
			return
		} else {
			attrs = append(attrs, attr)
		}
	}

	return attrs
}
