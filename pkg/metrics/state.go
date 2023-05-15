package metrics

import "github.com/prometheus/client_golang/prometheus"

const stateSubsystem = "state"

type stateMetrics struct {
	healthCheck prometheus.Gauge
}

func newStateMetrics() stateMetrics {
	return stateMetrics{
		healthCheck: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: stateSubsystem,
			Name:      "health",
			Help:      "Current Node state",
		}),
	}
}

func (m stateMetrics) register() {
	prometheus.MustRegister(m.healthCheck)
}

func (m stateMetrics) SetHealth(s int32) {
	m.healthCheck.Set(float64(s))
}
