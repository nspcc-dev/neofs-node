package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const writecacheSubsystem = "writecache"

type writecacheMetrics struct {
	putDuration         prometheus.HistogramVec
	flushSingleDuration prometheus.HistogramVec
	flushBatchDuration  prometheus.HistogramVec

	objectCount prometheus.GaugeVec
	size        prometheus.GaugeVec
}

func newWritecacheMetrics() writecacheMetrics {
	var (
		putDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: writecacheSubsystem,
			Name:      "put_time",
			Help:      "Writecache 'put' operations handling time",
		}, []string{shardIDLabelKey})

		flushSingleDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: writecacheSubsystem,
			Name:      "flush_single_time",
			Help:      "Writecache 'flush single' operations handling time",
		}, []string{shardIDLabelKey})

		flushBatchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: writecacheSubsystem,
			Name:      "flush_batch_time",
			Help:      "Writecache 'flush batch' operations handling time",
		}, []string{shardIDLabelKey})

		objectCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: writecacheSubsystem,
			Name:      "object_count",
			Help:      "Number of objects in the writecache",
		}, []string{shardIDLabelKey})

		size = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: writecacheSubsystem,
			Name:      "size",
			Help:      "Size of the writecache",
		}, []string{shardIDLabelKey})
	)
	return writecacheMetrics{
		putDuration:         *putDuration,
		flushSingleDuration: *flushSingleDuration,
		flushBatchDuration:  *flushBatchDuration,
		objectCount:         *objectCount,
		size:                *size,
	}
}

func (m writecacheMetrics) register() {
	prometheus.MustRegister(m.putDuration)
	prometheus.MustRegister(m.flushSingleDuration)
	prometheus.MustRegister(m.flushBatchDuration)
	prometheus.MustRegister(m.objectCount)
	prometheus.MustRegister(m.size)
}

func (m writecacheMetrics) AddWCPutDuration(shardID string, d time.Duration) {
	m.putDuration.With(prometheus.Labels{shardIDLabelKey: shardID}).Observe(d.Seconds())
}

func (m writecacheMetrics) AddWCFlushSingleDuration(shardID string, d time.Duration) {
	m.flushSingleDuration.With(prometheus.Labels{shardIDLabelKey: shardID}).Observe(d.Seconds())
}

func (m writecacheMetrics) AddWCFlushBatchDuration(shardID string, d time.Duration) {
	m.flushBatchDuration.With(prometheus.Labels{shardIDLabelKey: shardID}).Observe(d.Seconds())
}

func (m writecacheMetrics) IncWCObjectCount(shardID string) {
	m.objectCount.With(prometheus.Labels{shardIDLabelKey: shardID}).Inc()
}

func (m writecacheMetrics) DecWCObjectCount(shardID string) {
	m.objectCount.With(prometheus.Labels{shardIDLabelKey: shardID}).Dec()
}

func (m writecacheMetrics) AddWCSize(shardID string, size uint64) {
	m.size.With(prometheus.Labels{shardIDLabelKey: shardID}).Add(float64(size))
}

func (m writecacheMetrics) SetWCSize(shardID string, size uint64) {
	m.size.With(prometheus.Labels{shardIDLabelKey: shardID}).Set(float64(size))
}
