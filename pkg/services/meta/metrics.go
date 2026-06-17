package meta

import "github.com/prometheus/client_golang/prometheus"

const (
	nameSpace = "neofs_node"
	subsystem = "metadata_chain"
)

type metrics struct {
	newBlockFetchTime prometheus.Histogram
	objAcceptTime     prometheus.Histogram
	objAcceptBlocks   prometheus.Histogram
}

func (m *Meta) addMetrics() {
	m.metrics = metrics{
		newBlockFetchTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: nameSpace,
			Subsystem: subsystem,
			Name:      "new_block_fetch_time",
			Help:      "Time to receive new block after the previous one, seconds",
			Buckets:   []float64{0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.15},
		}),
		objAcceptTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: nameSpace,
			Subsystem: subsystem,
			Name:      "obj_accept_time",
			Help:      "Time b/w object is sent to chain and transaction with it is received, seconds",
			Buckets:   []float64{0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.15},
		}),
		objAcceptBlocks: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: nameSpace,
			Subsystem: subsystem,
			Name:      "obj_accept_blocks",
			Help:      "Number of blocks b/w object is sent to chain and transaction with it is received, blocks",
			Buckets:   []float64{0.5, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}),
	}

	prometheus.MustRegister(m.metrics.newBlockFetchTime, m.metrics.objAcceptTime, m.metrics.objAcceptBlocks)
}
