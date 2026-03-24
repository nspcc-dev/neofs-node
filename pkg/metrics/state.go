package metrics

import "github.com/prometheus/client_golang/prometheus"

const stateSubsystem = "state"

type stateMetrics struct {
	healthCheck             prometheus.Gauge
	policerConsistencyState prometheus.Gauge
	policerOptimalPlacement prometheus.Gauge
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
		policerOptimalPlacement: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: stateSubsystem,
			Name:      "policer_optimal_placement",
			Help:      "Current Policer's optimal placement state. 0 for non-optimal/unknown placement; 1 for optimal placement",
		}),
	}
}

func (m stateMetrics) register() {
	prometheus.MustRegister(m.healthCheck)
	prometheus.MustRegister(m.policerConsistencyState)
	prometheus.MustRegister(m.policerOptimalPlacement)
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

func (m stateMetrics) SetPolicerOptimalPlacementState(optimal bool) {
	if optimal {
		m.policerOptimalPlacement.Set(1)
	} else {
		m.policerOptimalPlacement.Set(0)
	}
}
