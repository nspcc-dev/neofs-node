package main

import (
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func initMetrics(c *cfg) *httputil.Server {
	if !c.appCfg.Prometheus.Enabled {
		c.log.Info("prometheus is disabled")
		return nil
	}

	var prm httputil.Prm

	prm.Address = c.appCfg.Prometheus.Address
	prm.Handler = promhttp.Handler()

	srv := httputil.New(prm,
		httputil.WithShutdownTimeout(
			c.appCfg.Prometheus.ShutdownTimeout,
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
		enabled:         c.Prometheus.Enabled,
		shutdownTimeout: c.Prometheus.ShutdownTimeout,
		address:         c.Prometheus.Address,
	}
}

func (m1 metricConfig) isUpdated(c *config.Config) bool {
	return m1.enabled != c.Prometheus.Enabled ||
		m1.shutdownTimeout != c.Prometheus.ShutdownTimeout ||
		m1.address != c.Prometheus.Address
}

func (c *cfg) setShardsCapacity() error {
	for _, sh := range c.cfgObject.cfgLocalStorage.localStorage.DumpInfo().Shards {
		path, err := getInitPath(sh.BlobStorInfo.Path)
		if err != nil {
			return err
		}

		capacity, err := totalBytes([]string{path})
		if err != nil {
			return fmt.Errorf("calculating capacity on shard %s: %w", sh.ID.String(), err)
		}

		c.metricsCollector.SetCapacitySize(sh.ID.String(), capacity)
	}

	return nil
}
