package main

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/stretchr/testify/require"
)

func TestValidateDefaultConfig(t *testing.T) {
	cfg, err := newConfig("")
	require.NoError(t, err)

	cfg.UnsetAll()
	require.Equal(t, *cfg, config.Config{
		Logger: config.Logger{
			Level:    "info",
			Encoding: "console",
		},
		WithoutMainnet: false,
		FSChain: config.Chain{
			BasicChain: config.BasicChain{
				DialTimeout:         60000000000,
				ReconnectionsNumber: 5,
				ReconnectionsDelay:  5000000000,
			},
			Validators: keys.PublicKeys{},
		},
		FSChainAutodeploy: false,
		NNS:               config.NNS{SystemEmail: ""},
		Mainnet: config.BasicChain{
			DialTimeout:         60000000000,
			ReconnectionsNumber: 5,
			ReconnectionsDelay:  5000000000,
		},
		Control: config.Control{
			AuthorizedKeys: keys.PublicKeys{},
			GRPC:           config.GRPC{Endpoint: ""},
		},
		Governance: config.Governance{Disable: false},
		Node: config.Node{
			PersistentState: config.PersistentState{
				Path: ".neofs-ir-state",
			},
		},
		Fee: config.Fee{MainChain: 50000000},
		Timers: config.Timers{
			CollectBasicIncome:    config.BasicTimer{Mul: 1, Div: 2},
			DistributeBasicIncome: config.BasicTimer{Mul: 3, Div: 4},
		},
		Emit: config.Emit{Storage: config.EmitStorage{Amount: 0},
			Mint: config.Mint{Value: 20000000, CacheSize: 1000, Threshold: 1},
			Gas:  config.Gas{BalanceThreshold: 0},
		},
		Workers: config.Workers{
			Alphabet:   10,
			Balance:    10,
			Container:  10,
			NeoFS:      10,
			Netmap:     10,
			Reputation: 10,
		},
		Indexer: config.Indexer{CacheTimeout: 15 * time.Second},
		Pprof: config.BasicService{
			Enabled:         false,
			Address:         "localhost:6060",
			ShutdownTimeout: 30000000000,
		},
		Prometheus: config.BasicService{
			Enabled:         false,
			Address:         "localhost:9090",
			ShutdownTimeout: 30000000000,
		},
		Validator: config.Validator{
			Enabled: false,
			URL:     "",
		},
		Settlement: config.Settlement{
			BasicIncomeRate: 0,
		},
		Experimental: config.Experimental{ChainMetaData: false}})
}
