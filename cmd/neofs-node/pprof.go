package main

import (
	profilerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/profiler"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
)

func pprofComponent(c *cfg) (*httpComponent, bool) {
	var updated bool
	// check if it has been inited before
	if c.dynamicConfiguration.pprof == nil {
		c.dynamicConfiguration.pprof = new(httpComponent)
		c.dynamicConfiguration.pprof.cfg = c
		c.dynamicConfiguration.pprof.name = "pprof"
		c.dynamicConfiguration.pprof.handler = httputil.Handler()
		updated = true
	}

	// (re)init read configuration
	enabled := profilerconfig.Enabled(c.appCfg)
	if enabled != c.dynamicConfiguration.pprof.enabled {
		c.dynamicConfiguration.pprof.enabled = enabled
		updated = true
	}
	address := profilerconfig.Address(c.appCfg)
	if address != c.dynamicConfiguration.pprof.address {
		c.dynamicConfiguration.pprof.address = address
		updated = true
	}
	dur := profilerconfig.ShutdownTimeout(c.appCfg)
	if dur != c.dynamicConfiguration.pprof.shutdownDur {
		c.dynamicConfiguration.pprof.shutdownDur = dur
		updated = true
	}

	return c.dynamicConfiguration.pprof, updated
}
