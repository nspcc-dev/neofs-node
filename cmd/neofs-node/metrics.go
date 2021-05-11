package main

import (
	"context"

	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func initMetrics(c *cfg) {
	addr := c.viper.GetString(cfgMetricsAddr)
	if addr == "" {
		return
	}

	var prm httputil.Prm

	prm.Address = addr
	prm.Handler = promhttp.Handler()

	srv := httputil.New(prm,
		httputil.WithShutdownTimeout(
			c.viper.GetDuration(cfgMetricsShutdownTimeout),
		),
	)

	c.workers = append(c.workers, newWorkerFromFunc(func(context.Context) {
		fatalOnErr(srv.Serve())
	}))

	c.closers = append(c.closers, func() {
		err := srv.Shutdown()
		if err != nil {
			c.log.Debug("could not shutdown metrics server",
				zap.String("error", err.Error()),
			)
		}
	})
}
