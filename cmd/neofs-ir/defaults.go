package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/internal/configutil"
	irconfig "github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/spf13/viper"
)

func newConfig(path string) (*irconfig.Config, error) {
	const innerRingPrefix = "neofs_ir"

	var (
		err error
		v   = viper.New()
	)

	v.SetEnvPrefix(innerRingPrefix)
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if path != "" {
		v.SetConfigFile(path)
		if strings.HasSuffix(path, ".json") {
			v.SetConfigType("json")
		} else {
			v.SetConfigType("yml")
		}
		err = v.ReadInConfig()
		if err != nil {
			return nil, err
		}
	}

	var cfg irconfig.Config
	err = configutil.Unmarshal(v, &cfg, innerRingPrefix)
	if err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}

	applyDefaults(&cfg)

	return &cfg, nil
}

func applyDefaults(cfg *irconfig.Config) {
	// Logger defaults
	if cfg.Logger.Level == "" {
		cfg.Logger.Level = "info"
	}
	if cfg.Logger.Encoding == "" {
		cfg.Logger.Encoding = "console"
	}

	// Pprof defaults
	if cfg.Pprof.Address == "" {
		cfg.Pprof.Address = "localhost:6060"
	}
	if cfg.Pprof.ShutdownTimeout == 0 {
		cfg.Pprof.ShutdownTimeout = 30 * time.Second
	}

	// Prometheus defaults
	if cfg.Prometheus.Address == "" {
		cfg.Prometheus.Address = "localhost:9090"
	}
	if cfg.Prometheus.ShutdownTimeout == 0 {
		cfg.Prometheus.ShutdownTimeout = 30 * time.Second
	}

	// Node defaults
	if cfg.Node.PersistentState.Path == "" {
		cfg.Node.PersistentState.Path = ".neofs-ir-state"
	}

	// FSChain defaults
	if cfg.FSChain.DialTimeout == 0 {
		cfg.FSChain.DialTimeout = time.Minute
	}
	if cfg.FSChain.ReconnectionsNumber == 0 {
		cfg.FSChain.ReconnectionsNumber = 5
	}
	if cfg.FSChain.ReconnectionsDelay == 0 {
		cfg.FSChain.ReconnectionsDelay = 5 * time.Second
	}
	if cfg.FSChain.Validators == nil {
		cfg.FSChain.Validators = keys.PublicKeys{}
	}

	// Mainnet defaults
	if cfg.Mainnet.ExtraFee == 0 {
		cfg.Mainnet.ExtraFee = 5000_0000 // 0.5 Fixed8
	}
	if cfg.Mainnet.DialTimeout == 0 {
		cfg.Mainnet.DialTimeout = time.Minute
	}
	if cfg.Mainnet.ReconnectionsNumber == 0 {
		cfg.Mainnet.ReconnectionsNumber = 5
	}
	if cfg.Mainnet.ReconnectionsDelay == 0 {
		cfg.Mainnet.ReconnectionsDelay = 5 * time.Second
	}

	// Timers defaults
	if cfg.Timers.CollectBasicIncome.Mul == 0 {
		cfg.Timers.CollectBasicIncome.Mul = 1
	}
	if cfg.Timers.CollectBasicIncome.Div == 0 {
		cfg.Timers.CollectBasicIncome.Div = 2
	}

	// Workers defaults
	if cfg.Workers.Netmap == 0 {
		cfg.Workers.Netmap = 10
	}
	if cfg.Workers.Balance == 0 {
		cfg.Workers.Balance = 10
	}
	if cfg.Workers.NeoFS == 0 {
		cfg.Workers.NeoFS = 10
	}
	if cfg.Workers.Container == 0 {
		cfg.Workers.Container = 10
	}
	if cfg.Workers.Alphabet == 0 {
		cfg.Workers.Alphabet = 10
	}
	if cfg.Workers.Reputation == 0 {
		cfg.Workers.Reputation = 10
	}

	// Emit defaults
	if cfg.Emit.Mint.CacheSize == 0 {
		cfg.Emit.Mint.CacheSize = 1000
	}
	if cfg.Emit.Mint.Threshold == 0 {
		cfg.Emit.Mint.Threshold = 1
	}
	if cfg.Emit.Mint.Value == 0 {
		cfg.Emit.Mint.Value = 20000000 // 0.2 Fixed8
	}

	// Indexer defaults
	if cfg.Indexer.CacheTimeout == 0 {
		cfg.Indexer.CacheTimeout = 15 * time.Second
	}

	// Control defaults
	if cfg.Control.AuthorizedKeys == nil {
		cfg.Control.AuthorizedKeys = keys.PublicKeys{}
	}
}
