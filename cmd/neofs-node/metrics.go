package main

import (
	metricsconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func metricsComponent(c *cfg) (*httpComponent, bool) {
	var updated bool
	// check if it has been inited before
	if c.dynamicConfiguration.metrics == nil {
		c.dynamicConfiguration.metrics = new(httpComponent)
		c.dynamicConfiguration.metrics.cfg = c
		c.dynamicConfiguration.metrics.name = "metrics"
		c.dynamicConfiguration.metrics.handler = promhttp.Handler()
		updated = true
	}

	// (re)init read configuration
	enabled := metricsconfig.Enabled(c.appCfg)
	if enabled != c.dynamicConfiguration.metrics.enabled {
		c.dynamicConfiguration.metrics.enabled = enabled
		updated = true
	}
	address := metricsconfig.Address(c.appCfg)
	if address != c.dynamicConfiguration.metrics.address {
		c.dynamicConfiguration.metrics.address = address
		updated = true
	}
	dur := metricsconfig.ShutdownTimeout(c.appCfg)
	if dur != c.dynamicConfiguration.metrics.shutdownDur {
		c.dynamicConfiguration.metrics.shutdownDur = dur
		updated = true
	}

	return c.dynamicConfiguration.metrics, updated
}

func enableMetricsSvc(c *cfg) {
	c.shared.metricsSvc.Enable()
}

func disableMetricsSvc(c *cfg) {
	c.shared.metricsSvc.Disable()
}
