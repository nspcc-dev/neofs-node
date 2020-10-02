package main

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/profiler"
)

func initProfiler(c *cfg) {
	c.profiler = profiler.NewProfiler(c.log, c.viper)
}

func serveProfiler(c *cfg) {
	if c.profiler != nil {
		c.profiler.Start(c.ctx)
	}
}
