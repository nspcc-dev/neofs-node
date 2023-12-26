package main

import (
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
