package main

import (
	"context"

	metricsconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/metrics"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func initMetrics(c *cfg) {
	addr := metricsconfig.Address(c.appCfg)
	if addr == "" {
		return
	}

	var prm httputil.Prm

	prm.Address = addr
	prm.Handler = promhttp.Handler()

	srv := httputil.New(prm,
		httputil.WithShutdownTimeout(
			metricsconfig.ShutdownTimeout(c.appCfg),
		),
	)

	c.workers = append(c.workers, newWorkerFromFunc(func(context.Context) {
		fatalOnErr(srv.Serve())
	}))

	c.closers = append(c.closers, func() {
		c.log.Debug("shutting down metrics service")

		err := srv.Shutdown()
		if err != nil {
			c.log.Debug("could not shutdown metrics server",
				zap.String("error", err.Error()),
			)
		}

		c.log.Debug("metrics service has been stopped")
	})
}
