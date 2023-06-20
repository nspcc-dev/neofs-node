package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const innerRingNameSpace = "neofs_ir"

// InnerRingServiceMetrics contains metrics collected by inner ring.
type InnerRingServiceMetrics struct {
	epoch prometheus.Gauge
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

	return InnerRingServiceMetrics{
		epoch: epoch,
	}
}

// SetEpoch updates epoch metrics.
func (m InnerRingServiceMetrics) SetEpoch(epoch uint64) {
	m.epoch.Set(float64(epoch))
}
