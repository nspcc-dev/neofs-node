package main

import (
	"context"
	"crypto/ecdsa"
	"net"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/util"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	tokenStorage "github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"google.golang.org/grpc"
)

type cfg struct {
	ctx context.Context

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
	endpoint string

	listener net.Listener

	server *grpc.Server
}

type cfgMorph struct {
	endpoint string

	client *client.Client
}

type cfgAccounting struct {
	scriptHash string

	fee util.Fixed8
}

type cfgContainer struct {
	scriptHash string

	fee util.Fixed8
}

type cfgNetmap struct {
	scriptHash string

	fee util.Fixed8
}

type BootstrapType uint32

type cfgNodeInfo struct {
	bootType BootstrapType

	address string
}

const (
	_ BootstrapType = iota
	StorageNode
	RelayNode
)

func defaultCfg() *cfg {
	key, err := crypto.LoadPrivateKey("Kwk6k2eC3L3QuPvD8aiaNyoSXgQ2YL1bwS5CP1oKoA9waeAze97s")
	fatalOnErr(err)

	return &cfg{
		ctx: context.Background(),
		wg:  new(sync.WaitGroup),
		key: key,
		cfgGRPC: cfgGRPC{
			endpoint: "127.0.0.1:50501",
		},
		cfgMorph: cfgMorph{
			endpoint: "http://morph_chain.localtest.nspcc.ru:30333/",
		},
		cfgAccounting: cfgAccounting{
			scriptHash: "1aeefe1d0dfade49740fff779c02cd4a0538ffb1",
			fee:        util.Fixed8(1),
		},
		cfgContainer: cfgContainer{
			scriptHash: "9d2ca84d7fb88213c4baced5a6ed4dc402309039",
			fee:        util.Fixed8(1),
		},
		cfgNetmap: cfgNetmap{
			scriptHash: "75194459637323ea8837d2afe8225ec74a5658c3",
			fee:        util.Fixed8(1),
		},
		cfgNodeInfo: cfgNodeInfo{
			bootType: StorageNode,
		},
	}
}
