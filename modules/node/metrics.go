package node

import (
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/lib/metrics"
	mService "github.com/nspcc-dev/neofs-node/services/metrics"
	"github.com/spf13/viper"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type (
	metricsParams struct {
		dig.In

		Logger  *zap.Logger
		Options []string `name:"node_options"`
		Viper   *viper.Viper
		Store   core.Storage
	}

	metricsServiceParams struct {
		dig.In

		Logger    *zap.Logger
		Collector metrics.Collector
	}
)

func newObjectCounter() *atomic.Float64 { return atomic.NewFloat64(0) }

func newMetricsService(p metricsServiceParams) (mService.Service, error) {
	return mService.New(mService.Params{
		Logger:    p.Logger,
		Collector: p.Collector,
	})
}

func newMetricsCollector(p metricsParams) (metrics.Collector, error) {
	store, err := p.Store.GetBucket(core.SpaceMetricsStore)
	if err != nil {
		return nil, err
	}

	return metrics.New(metrics.Params{
		Options:      p.Options,
		Logger:       p.Logger,
		Interval:     p.Viper.GetDuration("metrics_collector.interval"),
		MetricsStore: store,
	})
}
