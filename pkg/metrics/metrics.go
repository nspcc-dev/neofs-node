package metrics

import "github.com/prometheus/client_golang/prometheus"

const namespace = "neofs_node"

type NodeMetrics struct {
	objectServiceMetrics
	engineMetrics
	stateMetrics
	epoch prometheus.Gauge
}

func NewNodeMetrics() *NodeMetrics {
	objectService := newObjectServiceMetrics()
	objectService.register()

	engine := newEngineMetrics()
	engine.register()

	state := newStateMetrics()
	state.register()

	epoch := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: innerRingSubsystem,
		Name:      "epoch",
		Help:      "Current epoch as seen by inner-ring node.",
	})
	prometheus.MustRegister(epoch)

	return &NodeMetrics{
		objectServiceMetrics: objectService,
		engineMetrics:        engine,
		stateMetrics:         state,
		epoch:                epoch,
	}
}

// SetEpoch updates epoch metric.
func (m *NodeMetrics) SetEpoch(epoch uint64) {
	m.epoch.Set(float64(epoch))
}
