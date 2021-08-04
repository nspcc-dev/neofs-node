package main

import (
	"context"

	profilerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/profiler"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
	"go.uber.org/zap"
)

func initProfiler(c *cfg) {
	addr := profilerconfig.Address(c.appCfg)
	if addr == "" {
		return
	}

	var prm httputil.Prm

	prm.Address = addr
	prm.Handler = httputil.Handler()

	srv := httputil.New(prm,
		httputil.WithShutdownTimeout(
			profilerconfig.ShutdownTimeout(c.appCfg),
		),
	)

	c.workers = append(c.workers, newWorkerFromFunc(func(context.Context) {
		fatalOnErr(srv.Serve())
	}))

	c.closers = append(c.closers, func() {
		c.log.Debug("shutting down profiling service")

		err := srv.Shutdown()
		if err != nil {
			c.log.Debug("could not shutdown pprof server",
				zap.String("error", err.Error()),
			)
		}

		c.log.Debug("profiling service has been stopped")
	})
}
