package main

import (
	"strings"

	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/spf13/viper"
)

func newConfig(path string) (*viper.Viper, error) {
	var (
		err error
		v   = viper.New()
	)

	v.SetEnvPrefix(misc.InnerRingPrefix)
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v.SetDefault("app.name", misc.InnerRingName)
	v.SetDefault("app.version", misc.Version)

	defaultConfiguration(v)

	if path != "" {
		v.SetConfigFile(path)
		v.SetConfigType("yml") // fixme: for now
		err = v.ReadInConfig()
	}

	return v, err
}

func defaultConfiguration(cfg *viper.Viper) {
	cfg.SetDefault("logger.level", "info")
	cfg.SetDefault("logger.format", "console")
	cfg.SetDefault("logger.trace_level", "fatal")
	cfg.SetDefault("logger.no_disclaimer", false)
	cfg.SetDefault("logger.sampling.initial", 1000)
	cfg.SetDefault("logger.sampling.thereafter", 1000)

	cfg.SetDefault("pprof.enabled", false)
	cfg.SetDefault("pprof.address", ":6060")
	cfg.SetDefault("pprof.shutdown_ttl", "30s")

	cfg.SetDefault("metrics.enabled", false)
	cfg.SetDefault("metrics.address", ":9090")
	cfg.SetDefault("metrics.shutdown_ttl", "30s")

	cfg.SetDefault("morph.endpoint.client", "")
	cfg.SetDefault("morph.endpoint.notification", "")
	cfg.SetDefault("morph.dial_timeout", "10s")
	cfg.SetDefault("morph.validators", []string{})

	cfg.SetDefault("mainnet.endpoint.client", "")
	cfg.SetDefault("mainnet.endpoint.notification", "")
	cfg.SetDefault("mainnet.dial_timeout", "10s")

	cfg.SetDefault("key", "") // inner ring node key

	cfg.SetDefault("contracts.netmap", "")
	cfg.SetDefault("contracts.neofs", "")
	cfg.SetDefault("contracts.balance", "")
	cfg.SetDefault("contracts.container", "")
	cfg.SetDefault("contracts.audit", "")
	// alphabet contracts
	cfg.SetDefault("contracts.alphabet.amount", 7)
	cfg.SetDefault("contracts.alphabet.az", "")
	cfg.SetDefault("contracts.alphabet.buky", "")
	cfg.SetDefault("contracts.alphabet.vedi", "")
	cfg.SetDefault("contracts.alphabet.glagoli", "")
	cfg.SetDefault("contracts.alphabet.dobro", "")
	cfg.SetDefault("contracts.alphabet.jest", "")
	cfg.SetDefault("contracts.alphabet.zhivete", "")
	// gas native contract in LE
	cfg.SetDefault("contracts.gas", "70e2301955bf1e74cbb31d18c2f96972abadb328")

	cfg.SetDefault("timers.epoch", "0")
	cfg.SetDefault("timers.emit", "0")
	cfg.SetDefault("timers.stop_estimation.mul", 1)
	cfg.SetDefault("timers.stop_estimation.div", 1)
	cfg.SetDefault("timers.collect_basic_income.mul", 1)
	cfg.SetDefault("timers.collect_basic_income.div", 1)
	cfg.SetDefault("timers.distribute_basic_income.mul", 1)
	cfg.SetDefault("timers.distribute_basic_income.div", 1)

	cfg.SetDefault("workers.netmap", "10")
	cfg.SetDefault("workers.balance", "10")
	cfg.SetDefault("workers.neofs", "10")
	cfg.SetDefault("workers.container", "10")
	cfg.SetDefault("workers.alphabet", "10")

	cfg.SetDefault("netmap_cleaner.enabled", false)
	cfg.SetDefault("netmap_cleaner.threshold", 3)

	cfg.SetDefault("emit.storage.amount", 0)
	cfg.SetDefault("emit.mint.cache_size", 1000)
	cfg.SetDefault("emit.mint.threshold", 1)
	cfg.SetDefault("emit.mint.value", 20000000) // 0.2 Fixed8

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

	cfg.SetDefault("locode.db.path", "")
}
