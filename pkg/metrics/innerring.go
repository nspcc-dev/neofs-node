package metrics

import "github.com/prometheus/client_golang/prometheus"

const innerRingSubsystem = "object"

// InnerRingServiceMetrics contains metrics collected by inner ring.
type InnerRingServiceMetrics struct {
	epoch prometheus.Gauge
}

// NewInnerRingMetrics returns new instance of metrics collectors for inner ring.
func NewInnerRingMetrics() InnerRingServiceMetrics {
	var (
		epoch = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: innerRingSubsystem,
			Name:      "epoch",
			Help:      "Current epoch as seen by inner-ring node.",
		})
	)

	prometheus.MustRegister(epoch)

	return InnerRingServiceMetrics{
		epoch: epoch,
	}
}

// SetEpoch updates epoch metrics.
func (m InnerRingServiceMetrics) SetEpoch(epoch uint64) {
	m.epoch.Set(float64(epoch))
}
