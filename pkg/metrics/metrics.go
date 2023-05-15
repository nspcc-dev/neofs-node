package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const storageNodeNameSpace = "neofs_node"

type NodeMetrics struct {
	objectServiceMetrics
	engineMetrics
	stateMetrics
	epoch           prometheus.Gauge
	epochDeprecated prometheus.Gauge
}

func NewNodeMetrics(version string) *NodeMetrics {
	registerVersionMetric(storageNodeNameSpace, version)

	objectService := newObjectServiceMetrics()
	objectService.register()

	engine := newEngineMetrics()
	engine.register()

	state := newStateMetrics()
	state.register()

	epoch := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: storageNodeNameSpace,
		Subsystem: stateSubsystem,
		Name:      "epoch",
		Help:      "Current epoch as seen by NeoFS node.",
	})
	prometheus.MustRegister(epoch)

	// FIXME: drop after v0.38.0 release: #2347.
	const stateSubsystemDeprecated = objectSubsystem
	epochDeprecated := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: storageNodeNameSpace,
		Subsystem: stateSubsystemDeprecated,
		Name:      "epoch",
		Help:      fmt.Sprintf("Current epoch as seen by inner-ring node. DEPRECATED: use [%s_%s_epoch] instead.", storageNodeNameSpace, stateSubsystem),
	})
	prometheus.MustRegister(epochDeprecated)

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
	m.epochDeprecated.Set(float64(epoch))
}
