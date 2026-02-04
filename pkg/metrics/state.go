package metrics

import "github.com/prometheus/client_golang/prometheus"

const stateSubsystem = "state"

type stateMetrics struct {
	healthCheck             prometheus.Gauge
	policerConsistencyState prometheus.Gauge
}

func newStateMetrics() stateMetrics {
	return stateMetrics{
		healthCheck: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: stateSubsystem,
			Name:      "health",
			Help:      "Current Node state",
		}),
		policerConsistencyState: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: stateSubsystem,
			Name:      "policer_consistency_state",
			Help:      "Current Policer's consistency state. 0 for inconsistent/unknown state; 1 for synchronized",
		}),
	}
}

func (m stateMetrics) register() {
	prometheus.MustRegister(m.healthCheck)
	prometheus.MustRegister(m.policerConsistencyState)
}

func (m stateMetrics) SetHealth(s int32) {
	m.healthCheck.Set(float64(s))
}

func (m stateMetrics) SetPolicerConsistencyState(consistent bool) {
	if consistent {
		m.policerConsistencyState.Set(1)
	} else {
		m.policerConsistencyState.Set(0)
	}
}
