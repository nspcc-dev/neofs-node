package main

import (
	"context"
	"sync"
)

type cfg struct {
	ctx context.Context

	wg *sync.WaitGroup

	grpcAddr string
}

func defaultCfg() *cfg {
	return &cfg{
		ctx:      context.Background(),
		wg:       new(sync.WaitGroup),
		grpcAddr: "127.0.0.1:50501",
	}
}
