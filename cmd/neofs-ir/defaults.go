package main

import (
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
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
	cfg.SetDefault("morph.magic_number", uint32(netmode.PrivNet))

	cfg.SetDefault("mainnet.endpoint.client", "")
	cfg.SetDefault("mainnet.endpoint.notification", "")
	cfg.SetDefault("mainnet.dial_timeout", "10s")
	cfg.SetDefault("mainnet.magic_number", uint32(netmode.PrivNet))

	cfg.SetDefault("key", "") // inner ring node key

	cfg.SetDefault("contracts.netmap", "")
	cfg.SetDefault("contracts.neofs", "")
	cfg.SetDefault("contracts.balance", "")
	cfg.SetDefault("contracts.container", "")
	// gas native contract in LE
	cfg.SetDefault("contracts.gas", "668e0c1f9d7b70a99dd9e06eadd4c784d641afbc")

	cfg.SetDefault("timers.epoch", "5s")

	cfg.SetDefault("workers.netmap", "10")
	cfg.SetDefault("workers.balance", "10")
	cfg.SetDefault("workers.neofs", "10")
	cfg.SetDefault("workers.container", "10")
}
