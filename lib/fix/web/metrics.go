package web

import (
	"context"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// Metrics is an interface of metric tool.
type Metrics interface {
	Start(ctx context.Context)
	Stop()
}

const metricsKey = "metrics"

// NewMetrics is a metric tool's constructor.
func NewMetrics(l *zap.Logger, v *viper.Viper) Metrics {
	if !v.GetBool(metricsKey + ".enabled") {
		l.Debug("metrics server disabled")
		return nil
	}

	return newHTTPServer(httpParams{
		Key:     metricsKey,
		Viper:   v,
		Logger:  l,
		Handler: promhttp.Handler(),
	})
}
