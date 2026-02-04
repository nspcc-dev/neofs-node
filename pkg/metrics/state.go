package metrics

import "github.com/prometheus/client_golang/prometheus"

const stateSubsystem = "state"

type stateMetrics struct {
	healthCheck            prometheus.Gauge
	engineConsistencyState prometheus.Gauge
}

func newStateMetrics() stateMetrics {
	return stateMetrics{
		healthCheck: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: stateSubsystem,
			Name:      "health",
			Help:      "Current Node state",
		}),
		engineConsistencyState: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: stateSubsystem,
			Name:      "engine_consistency_state",
			Help:      "Current storage engine's consistency state. 0 for inconsistent/unknown state; 1 for synchronized",
		}),
	}
}

func (m stateMetrics) register() {
	prometheus.MustRegister(m.healthCheck)
	prometheus.MustRegister(m.engineConsistencyState)
}

func (m stateMetrics) SetHealth(s int32) {
	m.healthCheck.Set(float64(s))
}

func (m stateMetrics) SetEngineConsistencyState(consistent bool) {
	if consistent {
		m.engineConsistencyState.Set(1)
	} else {
		m.engineConsistencyState.Set(0)
	}
}
