package main

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/profiler"
)

func initMetrics(c *cfg) {
	c.metrics = profiler.NewMetrics(c.log, c.viper)
}

func serveMetrics(c *cfg) {
	if c.metrics != nil {
		c.metrics.Start(c.ctx)
	}
}
