package main

import (
	"context"
	"crypto/ecdsa"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/util"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"google.golang.org/grpc"
)

type cfg struct {
	ctx context.Context

	wg *sync.WaitGroup

	grpcAddr string

	key *ecdsa.PrivateKey

	grpcSrv *grpc.Server

	morphEndpoint string

	morphClient *client.Client

	cfgAccounting *cfgAccounting
}

func defaultCfg() *cfg {
	key, err := crypto.LoadPrivateKey("Kwk6k2eC3L3QuPvD8aiaNyoSXgQ2YL1bwS5CP1oKoA9waeAze97s")
	fatalOnErr(err)

	return &cfg{
		ctx:           context.Background(),
		wg:            new(sync.WaitGroup),
		grpcAddr:      "127.0.0.1:50501",
		key:           key,
		morphEndpoint: "http://morph_chain.localtest.nspcc.ru:30333/",
		cfgAccounting: &cfgAccounting{
			scriptHash: "1aeefe1d0dfade49740fff779c02cd4a0538ffb1",
			fee:        util.Fixed8(1),
		},
	}
}
