package metrics

import "github.com/prometheus/client_golang/prometheus"

const namespace = "neofs_node"

type StorageMetrics struct {
	objectServiceMetrics
	engineMetrics
	epoch prometheus.Gauge
}

func NewStorageMetrics() *StorageMetrics {
	objectService := newObjectServiceMetrics()
	objectService.register()

	engine := newEngineMetrics()
	engine.register()

	epoch := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: innerRingSubsystem,
		Name:      "epoch",
		Help:      "Current epoch as seen by inner-ring node.",
	})
	prometheus.MustRegister(epoch)

	return &StorageMetrics{
		objectServiceMetrics: objectService,
		engineMetrics:        engine,
		epoch:                epoch,
	}
}

// SetEpoch updates epoch metric.
func (m *StorageMetrics) SetEpoch(epoch uint64) {
	m.epoch.Set(float64(epoch))
}
