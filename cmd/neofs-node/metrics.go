package main

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/profiler"
)

func initMetrics(c *cfg) {
	if c.metricsCollector != nil {
		c.metricsServer = profiler.NewMetrics(c.log, c.viper)
	}
}

func serveMetrics(c *cfg) {
	if c.metricsServer != nil {
		c.metricsServer.Start(c.ctx)
	}
}
