package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const storageNodeNameSpace = "neofs_node"

type NodeMetrics struct {
	objectServiceMetrics
	engineMetrics
	stateMetrics
	writecacheMetrics
	epoch prometheus.Gauge
}

// readPathBuckets are bounds of histograms timing operations that normally
// take much less than the 5 ms [prometheus.DefBuckets] start at, such as
// reading a header. They are the default bounds with finer ones in front.
var readPathBuckets = append([]float64{.000025, .00005, .0001, .00025, .0005, .001, .0025},
	prometheus.DefBuckets...)

func NewNodeMetrics(version string) *NodeMetrics {
	registerVersionMetric(storageNodeNameSpace, version)

	objectService := newObjectServiceMetrics()
	objectService.register()

	engine := newEngineMetrics()
	engine.register()

	state := newStateMetrics()
	state.register()

	writecache := newWritecacheMetrics()
	writecache.register()

	epoch := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: storageNodeNameSpace,
		Subsystem: stateSubsystem,
		Name:      "epoch",
		Help:      "Current epoch as seen by NeoFS node.",
	})
	prometheus.MustRegister(epoch)

	return &NodeMetrics{
		objectServiceMetrics: objectService,
		engineMetrics:        engine,
		stateMetrics:         state,
		writecacheMetrics:    writecache,
		epoch:                epoch,
	}
}

// SetEpoch updates epoch metric.
func (m *NodeMetrics) SetEpoch(epoch uint64) {
	m.epoch.Set(float64(epoch))
}
