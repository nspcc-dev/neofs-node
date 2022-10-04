package main

import (
	"context"

	profilerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/profiler"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

func initProfiler(c *cfg) {
	if !profilerconfig.Enabled(c.appCfg) {
		c.log.Info("pprof is disabled")
		return
	}

	var prm httputil.Prm

	prm.Address = profilerconfig.Address(c.appCfg)
	prm.Handler = httputil.Handler()

	srv := httputil.New(prm,
		httputil.WithShutdownTimeout(
			profilerconfig.ShutdownTimeout(c.appCfg),
		),
	)

	c.workers = append(c.workers, newWorkerFromFunc(func(context.Context) {
		runAndLog(c, "profiler", false, func(c *cfg) {
			fatalOnErr(srv.Serve())
		})
	}))

	c.closers = append(c.closers, func() {
		c.log.Debug("shutting down profiling service")

		err := srv.Shutdown()
		if err != nil {
			c.log.Debug("could not shutdown pprof server",
				logger.FieldError(err),
			)
		}

		c.log.Debug("profiling service has been stopped")
	})
}
