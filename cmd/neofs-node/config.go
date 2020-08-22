package main

import (
	"context"
	"crypto/ecdsa"
	"sync"

	crypto "github.com/nspcc-dev/neofs-crypto"
)

type cfg struct {
	ctx context.Context

	wg *sync.WaitGroup

	grpcAddr string

	key *ecdsa.PrivateKey
}

func defaultCfg() *cfg {
	key, err := crypto.LoadPrivateKey("Kwk6k2eC3L3QuPvD8aiaNyoSXgQ2YL1bwS5CP1oKoA9waeAze97s")
	fatalOnErr(err)

	return &cfg{
		ctx:      context.Background(),
		wg:       new(sync.WaitGroup),
		grpcAddr: "127.0.0.1:50501",
		key:      key,
	}
}
