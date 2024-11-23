package main

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	profilerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/profiler"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
)

func initProfiler(c *cfg) *httputil.Server {
	if !profilerconfig.Enabled(c.cfgReader) {
		c.log.Info("pprof is disabled")
		return nil
	}

	var prm httputil.Prm

	prm.Address = profilerconfig.Address(c.cfgReader)
	prm.Handler = httputil.Handler()

	srv := httputil.New(prm,
		httputil.WithShutdownTimeout(
			profilerconfig.ShutdownTimeout(c.cfgReader),
		),
	)

	return srv
}

type profilerConfig struct {
	enabled         bool
	shutdownTimeout time.Duration
	address         string
}

func writeProfilerConfig(c *config.Config) profilerConfig {
	return profilerConfig{
		enabled:         profilerconfig.Enabled(c),
		shutdownTimeout: profilerconfig.ShutdownTimeout(c),
		address:         profilerconfig.Address(c),
	}
}

func (m1 profilerConfig) isUpdated(c *config.Config) bool {
	return m1.enabled != profilerconfig.Enabled(c) ||
		m1.shutdownTimeout != profilerconfig.ShutdownTimeout(c) ||
		m1.address != profilerconfig.Address(c)
}
