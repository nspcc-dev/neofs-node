package node

import (
	metrics "github.com/nspcc-dev/neofs-node/pkg/network/transport/metrics/grpc"
	metrics2 "github.com/nspcc-dev/neofs-node/pkg/services/metrics"
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
		Buckets Buckets
	}

	metricsServiceParams struct {
		dig.In

		Logger    *zap.Logger
		Collector metrics2.Collector
	}
)

func newObjectCounter() *atomic.Float64 { return atomic.NewFloat64(0) }

func newMetricsService(p metricsServiceParams) (metrics.Service, error) {
	return metrics.New(metrics.Params{
		Logger:    p.Logger,
		Collector: p.Collector,
	})
}

func newMetricsCollector(p metricsParams) (metrics2.Collector, error) {
	return metrics2.New(metrics2.Params{
		Options:      p.Options,
		Logger:       p.Logger,
		Interval:     p.Viper.GetDuration("metrics_collector.interval"),
		MetricsStore: p.Buckets[fsBucket],
	})
}
