package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const innerRingNameSpace = "neofs_ir"

// FIXME: drop after v0.38.0 release: #2347.
const innerRingNameSpaceDeprecated = storageNodeNameSpace
const innerRingSubsystemDeprecated = objectSubsystem

// InnerRingServiceMetrics contains metrics collected by inner ring.
type InnerRingServiceMetrics struct {
	epoch           prometheus.Gauge
	epochDeprecated prometheus.Gauge
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

	epochDeprecated := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: innerRingNameSpaceDeprecated,
		Subsystem: innerRingSubsystemDeprecated,
		Name:      "epoch",
		Help:      fmt.Sprintf("Current epoch as seen by inner-ring node. DEPRECATED: use [%s_%s_epoch] instead.", innerRingNameSpace, stateSubsystem),
	})
	prometheus.MustRegister(epochDeprecated)

	return InnerRingServiceMetrics{
		epoch:           epoch,
		epochDeprecated: epochDeprecated,
	}
}

// SetEpoch updates epoch metrics.
func (m InnerRingServiceMetrics) SetEpoch(epoch uint64) {
	m.epoch.Set(float64(epoch))
	m.epochDeprecated.Set(float64(epoch))
}
