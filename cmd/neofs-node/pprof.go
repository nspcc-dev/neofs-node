package main

import (
	"runtime"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	pprofconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/pprof"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
)

func initProfiler(c *cfg) *httputil.Server {
	if !c.appCfg.Pprof.Enabled {
		c.log.Info("pprof is disabled")
		return nil
	}

	var prm httputil.Prm

	prm.Address = c.appCfg.Pprof.Address
	prm.Handler = httputil.Handler()

	srv := httputil.New(prm,
		httputil.WithShutdownTimeout(
			c.appCfg.Pprof.ShutdownTimeout,
		),
	)

	tuneProfiles(c.appCfg.Pprof)

	return srv
}

type profilerConfig struct {
	enabled         bool
	shutdownTimeout time.Duration
	address         string
}

func writeProfilerConfig(c *config.Config) profilerConfig {
	return profilerConfig{
		enabled:         c.Pprof.Enabled,
		shutdownTimeout: c.Pprof.ShutdownTimeout,
		address:         c.Pprof.Address,
	}
}

func (m1 profilerConfig) isUpdated(c *config.Config) bool {
	return m1.enabled != c.Pprof.Enabled ||
		m1.shutdownTimeout != c.Pprof.ShutdownTimeout ||
		m1.address != c.Pprof.Address
}

func tuneProfiles(cfg pprofconfig.Pprof) {
	if cfg.EnableBlock {
		runtime.SetBlockProfileRate(1)
	} else {
		runtime.SetBlockProfileRate(0)
	}

	if cfg.EnableMutex {
		runtime.SetMutexProfileFraction(1)
	} else {
		runtime.SetMutexProfileFraction(0)
	}
}
