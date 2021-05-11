package main

import (
	"context"

	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
	"go.uber.org/zap"
)

func initProfiler(c *cfg) {
	addr := c.viper.GetString(cfgProfilerAddr)
	if addr == "" {
		return
	}

	var prm httputil.Prm

	prm.Address = addr
	prm.Handler = httputil.Handler()

	srv := httputil.New(prm,
		httputil.WithShutdownTimeout(
			c.viper.GetDuration(cfgProfilerShutdownTimeout),
		),
	)

	c.workers = append(c.workers, newWorkerFromFunc(func(context.Context) {
		fatalOnErr(srv.Serve())
	}))

	c.closers = append(c.closers, func() {
		err := srv.Shutdown()
		if err != nil {
			c.log.Debug("could not shutdown pprof server",
				zap.String("error", err.Error()),
			)
		}
	})
}
