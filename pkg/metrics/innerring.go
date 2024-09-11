package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const innerRingNameSpace = "neofs_ir"

// InnerRingServiceMetrics contains metrics collected by inner ring.
type InnerRingServiceMetrics struct {
	epoch       prometheus.Gauge
	healthCheck prometheus.Gauge
}

// NewInnerRingMetrics returns new instance of metrics collectors for inner ring.
func NewInnerRingMetrics(version string) InnerRingServiceMetrics {
	registerVersionMetric(innerRingNameSpace, version)

	epoch := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: innerRingNameSpace,
		Subsystem: stateSubsystem,
		Name:      "epoch",
		Help:      "Current epoch as seen by inner-ring node.",
	})
	prometheus.MustRegister(epoch)

	healthCheck := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: innerRingNameSpace,
		Subsystem: stateSubsystem,
		Name:      "health",
		Help:      "Current ir state",
	})
	prometheus.MustRegister(healthCheck)

	return InnerRingServiceMetrics{
		epoch:       epoch,
		healthCheck: healthCheck,
	}
}

// SetEpoch updates epoch metrics.
func (m InnerRingServiceMetrics) SetEpoch(epoch uint64) {
	m.epoch.Set(float64(epoch))
}

func (m InnerRingServiceMetrics) SetHealthCheck(healthCheck int32) {
	m.healthCheck.Set(float64(healthCheck))
}
