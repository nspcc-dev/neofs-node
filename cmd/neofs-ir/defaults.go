package main

import (
	"fmt"
	"strings"
	"time"

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

	defaultConfiguration(v)

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

	return &cfg, nil
}

func defaultConfiguration(cfg *viper.Viper) {
	cfg.SetDefault("logger.level", "info")
	cfg.SetDefault("logger.encoding", "console")

	cfg.SetDefault("pprof.address", "localhost:6060")
	cfg.SetDefault("pprof.shutdown_timeout", "30s")

	cfg.SetDefault("prometheus.address", "localhost:9090")
	cfg.SetDefault("prometheus.shutdown_timeout", "30s")

	cfg.SetDefault("without_mainnet", false)

	cfg.SetDefault("node.persistent_state.path", ".neofs-ir-state")

	cfg.SetDefault("fschain.dial_timeout", time.Minute)
	cfg.SetDefault("fschain.reconnections_number", 5)
	cfg.SetDefault("fschain.reconnections_delay", 5*time.Second)
	cfg.SetDefault("fschain.validators", []string{})

	cfg.SetDefault("mainnet.dial_timeout", time.Minute)
	cfg.SetDefault("mainnet.reconnections_number", 5)
	cfg.SetDefault("mainnet.reconnections_delay", 5*time.Second)

	cfg.SetDefault("wallet.path", "")     // inner ring node NEP-6 wallet
	cfg.SetDefault("wallet.address", "")  // account address
	cfg.SetDefault("wallet.password", "") // password

	cfg.SetDefault("timers.collect_basic_income.mul", 1)
	cfg.SetDefault("timers.collect_basic_income.div", 2)
	cfg.SetDefault("timers.distribute_basic_income.mul", 3)
	cfg.SetDefault("timers.distribute_basic_income.div", 4)

	cfg.SetDefault("workers.netmap", "10")
	cfg.SetDefault("workers.balance", "10")
	cfg.SetDefault("workers.neofs", "10")
	cfg.SetDefault("workers.container", "10")
	cfg.SetDefault("workers.alphabet", "10")
	cfg.SetDefault("workers.reputation", "10")

	cfg.SetDefault("emit.storage.amount", 0)
	cfg.SetDefault("emit.mint.cache_size", 1000)
	cfg.SetDefault("emit.mint.threshold", 1)
	cfg.SetDefault("emit.mint.value", 20000000) // 0.2 Fixed8
	cfg.SetDefault("emit.gas.balance_threshold", 0)

	cfg.SetDefault("settlement.basic_income_rate", 0)

	cfg.SetDefault("indexer.cache_timeout", 15*time.Second)

	// extra fee values for working mode without notary contract
	cfg.SetDefault("fee.main_chain", 5000_0000) // 0.5 Fixed8

	cfg.SetDefault("control.authorized_keys", []string{})
	cfg.SetDefault("control.grpc.endpoint", "")

	cfg.SetDefault("governance.disable", false)
}
