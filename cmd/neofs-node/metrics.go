package main

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	metricsconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/metrics"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func initMetrics(c *cfg) *httputil.Server {
	if !metricsconfig.Enabled(c.cfgReader) {
		c.log.Info("prometheus is disabled")
		return nil
	}

	var prm httputil.Prm

	prm.Address = metricsconfig.Address(c.cfgReader)
	prm.Handler = promhttp.Handler()

	srv := httputil.New(prm,
		httputil.WithShutdownTimeout(
			metricsconfig.ShutdownTimeout(c.cfgReader),
		),
	)

	return srv
}

type metricConfig struct {
	enabled         bool
	shutdownTimeout time.Duration
	address         string
}

func writeMetricConfig(c *config.Config) metricConfig {
	return metricConfig{
		enabled:         metricsconfig.Enabled(c),
		shutdownTimeout: metricsconfig.ShutdownTimeout(c),
		address:         metricsconfig.Address(c),
	}
}

func (m1 metricConfig) isUpdated(c *config.Config) bool {
	return m1.enabled != metricsconfig.Enabled(c) ||
		m1.shutdownTimeout != metricsconfig.ShutdownTimeout(c) ||
		m1.address != metricsconfig.Address(c)
}
