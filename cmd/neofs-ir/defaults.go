package main

import (
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-ir/internal/validate"
	"github.com/spf13/viper"
)

func newConfig(path string) (*viper.Viper, error) {
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

	err = validate.ValidateStruct(v)
	if err != nil {
		return nil, err
	}

	return v, nil
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

	cfg.SetDefault("morph.dial_timeout", time.Minute)
	cfg.SetDefault("morph.reconnections_number", 5)
	cfg.SetDefault("morph.reconnections_delay", 5*time.Second)
	cfg.SetDefault("morph.validators", []string{})
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

	cfg.SetDefault("timers.stop_estimation.mul", 1)
	cfg.SetDefault("timers.stop_estimation.div", 4)
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

	cfg.SetDefault("netmap_cleaner.enabled", true)
	cfg.SetDefault("netmap_cleaner.threshold", 3)

	cfg.SetDefault("emit.storage.amount", 0)
	cfg.SetDefault("emit.mint.cache_size", 1000)
	cfg.SetDefault("emit.mint.threshold", 1)
	cfg.SetDefault("emit.mint.value", 20000000) // 0.2 Fixed8
	cfg.SetDefault("emit.gas.balance_threshold", 0)

	cfg.SetDefault("audit.task.exec_pool_size", 10)
	cfg.SetDefault("audit.task.queue_capacity", 100)
	cfg.SetDefault("audit.timeout.get", "5s")
	cfg.SetDefault("audit.timeout.head", "5s")
	cfg.SetDefault("audit.timeout.rangehash", "5s")
	cfg.SetDefault("audit.timeout.search", "10s")
	cfg.SetDefault("audit.pdp.max_sleep_interval", "5s")
	cfg.SetDefault("audit.pdp.pairs_pool_size", "10")
	cfg.SetDefault("audit.por.pool_size", "10")

	cfg.SetDefault("settlement.basic_income_rate", 0)
	cfg.SetDefault("settlement.audit_fee", 0)

	cfg.SetDefault("indexer.cache_timeout", 15*time.Second)

	// extra fee values for working mode without notary contract
	cfg.SetDefault("fee.main_chain", 5000_0000) // 0.5 Fixed8

	cfg.SetDefault("control.authorized_keys", []string{})
	cfg.SetDefault("control.grpc.endpoint", "")

	cfg.SetDefault("governance.disable", false)
}
