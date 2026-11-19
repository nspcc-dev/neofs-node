package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const stateSubsystem = "state"

type stateMetrics struct {
	healthCheck             prometheus.Gauge
	policerConsistencyState prometheus.Gauge
	policerOptimalPlacement prometheus.Gauge
	policerCycleCounter     prometheus.Counter
	policerObjectCounter    *prometheus.CounterVec
	policerHeadDuration     prometheus.Histogram
	policerHeadCounter      *prometheus.CounterVec
	// policerHeadResults are children of policerHeadCounter, resolved once.
	policerHeadResults map[string]prometheus.Counter
}

// Results of Policer's HEAD requests, see [stateMetrics.HandlePolicerHead].
const (
	PolicerHeadOK          = "ok"
	PolicerHeadNotFound    = "not_found"
	PolicerHeadMaintenance = "maintenance"
	PolicerHeadTimeout     = "timeout"
	PolicerHeadError       = "error"
)

const (
	policerActionLabel = "action"
	policerModeLabel   = "mode"
	policerResultLabel = "result"
)

func newStateMetrics() stateMetrics {
	m := stateMetrics{
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
		policerCycleCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: stateSubsystem,
			Name:      "policer_cycle_count",
			Help:      "Number of completed Policer local storage cycles",
		}),
		policerObjectCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: stateSubsystem,
			Name:      "policer_object_count",
			Help:      "Number of objects processed by Policer grouped by action and mode",
		}, []string{policerActionLabel, policerModeLabel}),
		policerHeadDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: stateSubsystem,
			Name:      "policer_head_time",
			Help:      "Time of HEAD requests Policer sent to other nodes, whatever their result",
			Buckets:   readPathBuckets,
		}),
		policerHeadCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: stateSubsystem,
			Name:      "policer_head_count",
			Help:      "Number of HEAD requests Policer sent to other nodes grouped by result",
		}, []string{policerResultLabel}),
	}
	m.policerHeadResults = make(map[string]prometheus.Counter)
	for _, r := range []string{PolicerHeadOK, PolicerHeadNotFound, PolicerHeadMaintenance, PolicerHeadTimeout, PolicerHeadError} {
		m.policerHeadResults[r] = m.policerHeadCounter.WithLabelValues(r)
	}
	return m
}

func (m stateMetrics) register() {
	prometheus.MustRegister(m.healthCheck)
	prometheus.MustRegister(m.policerConsistencyState)
	prometheus.MustRegister(m.policerOptimalPlacement)
	prometheus.MustRegister(m.policerCycleCounter)
	prometheus.MustRegister(m.policerObjectCounter)
	prometheus.MustRegister(m.policerHeadDuration)
	prometheus.MustRegister(m.policerHeadCounter)
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

func (m stateMetrics) IncPolicerCycleCount() {
	m.policerCycleCounter.Inc()
}

func (m stateMetrics) IncPolicerObjectProcessed(isEC bool) {
	m.incPolicerObjectCounter("processed", isEC)
}

func (m stateMetrics) IncPolicerObjectReplicated(isEC bool) {
	m.incPolicerObjectCounter("replicated", isEC)
}

func (m stateMetrics) IncPolicerObjectDeleted(isEC bool) {
	m.incPolicerObjectCounter("deleted", isEC)
}

func (m stateMetrics) IncPolicerObjectRelocated() {
	m.policerObjectCounter.With(prometheus.Labels{
		policerActionLabel: "relocated",
		policerModeLabel:   "physical",
	}).Inc()
}

// HandlePolicerHead counts a HEAD request Policer sent to another node by
// its result, one of the PolicerHead* values, and observes its duration.
func (m stateMetrics) HandlePolicerHead(result string, d time.Duration) {
	m.policerHeadDuration.Observe(d.Seconds())
	m.policerHeadResults[result].Inc()
}

func (m stateMetrics) incPolicerObjectCounter(action string, isEC bool) {
	mode := "replica"
	if isEC {
		mode = "ec"
	}

	m.policerObjectCounter.With(prometheus.Labels{
		policerActionLabel: action,
		policerModeLabel:   mode,
	}).Inc()
}
