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
		}),
		objAcceptTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: nameSpace,
			Subsystem: subsystem,
			Name:      "obj_accept_time",
			Help:      "Time b/w object is sent to chain and transaction with it is received, seconds",
		}),
		objAcceptBlocks: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: nameSpace,
			Subsystem: subsystem,
			Name:      "obj_accept_blocks",
			Help:      "Number of blocks b/w object is sent to chain and transaction with it is received, blocks",
		}),
	}

	prometheus.MustRegister(m.metrics.newBlockFetchTime, m.metrics.objAcceptTime, m.metrics.objAcceptBlocks)
}
