package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type (
	engineMetrics struct {
		listContainersDuration        prometheus.Histogram
		estimateContainerSizeDuration prometheus.Histogram
		deleteDuration                prometheus.Histogram
		dropDuration                  prometheus.Histogram
		existsDuration                prometheus.Histogram
		getDuration                   prometheus.Histogram
		headDuration                  prometheus.Histogram
		getStreamDuration             prometheus.Histogram
		getRangeStreamDuration        prometheus.Histogram
		inhumeDuration                prometheus.Histogram
		putDuration                   prometheus.Histogram
		rangeDuration                 prometheus.Histogram
		searchDuration                prometheus.Histogram
		listObjectsDuration           prometheus.Histogram
		getECPartDuration             prometheus.Histogram
		getECPartRangeDuration        prometheus.Histogram
		headECPartDuration            prometheus.Histogram

		containerSize prometheus.GaugeVec
		payloadSize   prometheus.GaugeVec
		capacitySize  prometheus.GaugeVec
	}
)

const engineSubsystem = "engine"

func newEngineMetrics() engineMetrics {
	var (
		listContainersDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "list_containers_time",
			Help:      "Engine 'list containers' operations handling time",
		})

		estimateContainerSizeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "estimate_container_size_time",
			Help:      "Engine 'container size' operations handling time",
		})

		deleteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "delete_time",
			Help:      "Engine 'delete' operations handling time",
		})

		dropDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "drop_time",
			Help:      "Engine 'drop' operations handling time",
		})

		existsDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "exists_time",
			Help:      "Engine 'exists' operations handling time",
		})

		getDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "get_time",
			Help:      "Engine 'get' operations handling time",
		})

		headDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "head_time",
			Help:      "Engine 'head' operations handling time",
		})

		getStreamDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "get_stream_time",
			Help:      "Engine 'get stream' operations handling time",
		})

		getRangeStreamDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "get_range_stream_time",
			Help:      "Engine 'get range stream' operations handling time",
		})

		inhumeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "inhume_time",
			Help:      "Engine 'inhume' operations handling time",
		})

		putDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "put_time",
			Help:      "Engine 'put' operations handling time",
		})

		rangeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "range_time",
			Help:      "Engine 'range' operations handling time",
		})

		searchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "search_time",
			Help:      "Engine 'search' operations handling time",
		})

		listObjectsDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "list_objects_time",
			Help:      "Engine 'list objects' operations handling time",
		})

		getECPartDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "get_ec_part_time",
			Help:      "Engine 'get EC part' operations handling time",
		})

		getECPartRangeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "get_ec_part__range_time",
			Help:      "Engine 'get EC part range' operations handling time",
		})

		headECPartDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "head_ec_part_time",
			Help:      "Engine 'head EC part' operations handling time",
		})
	)

	var (
		containerSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "container_size",
			Help:      "Accumulated size of all objects in a container",
		}, []string{containerIDLabelKey})

		payloadSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "payload_size",
			Help:      "Accumulated size of all objects in a shard",
		}, []string{shardIDLabelKey})

		capacitySize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "capacity",
			Help:      "Contains the shard's capacity",
		}, []string{shardIDLabelKey})
	)

	return engineMetrics{
		listContainersDuration:        listContainersDuration,
		estimateContainerSizeDuration: estimateContainerSizeDuration,
		deleteDuration:                deleteDuration,
		dropDuration:                  dropDuration,
		existsDuration:                existsDuration,
		getDuration:                   getDuration,
		headDuration:                  headDuration,
		getStreamDuration:             getStreamDuration,
		getRangeStreamDuration:        getRangeStreamDuration,
		inhumeDuration:                inhumeDuration,
		putDuration:                   putDuration,
		rangeDuration:                 rangeDuration,
		searchDuration:                searchDuration,
		listObjectsDuration:           listObjectsDuration,
		getECPartDuration:             getECPartDuration,
		getECPartRangeDuration:        getECPartRangeDuration,
		headECPartDuration:            headECPartDuration,
		containerSize:                 *containerSize,
		payloadSize:                   *payloadSize,
		capacitySize:                  *capacitySize,
	}
}

func (m engineMetrics) register() {
	prometheus.MustRegister(m.listContainersDuration)
	prometheus.MustRegister(m.estimateContainerSizeDuration)
	prometheus.MustRegister(m.deleteDuration)
	prometheus.MustRegister(m.dropDuration)
	prometheus.MustRegister(m.existsDuration)
	prometheus.MustRegister(m.getDuration)
	prometheus.MustRegister(m.headDuration)
	prometheus.MustRegister(m.getStreamDuration)
	prometheus.MustRegister(m.getRangeStreamDuration)
	prometheus.MustRegister(m.inhumeDuration)
	prometheus.MustRegister(m.putDuration)
	prometheus.MustRegister(m.rangeDuration)
	prometheus.MustRegister(m.searchDuration)
	prometheus.MustRegister(m.listObjectsDuration)
	prometheus.MustRegister(m.getECPartDuration)
	prometheus.MustRegister(m.getECPartRangeDuration)
	prometheus.MustRegister(m.headECPartDuration)
	prometheus.MustRegister(m.containerSize)
	prometheus.MustRegister(m.payloadSize)
	prometheus.MustRegister(m.capacitySize)
}

func (m engineMetrics) AddListContainersDuration(d time.Duration) {
	m.listContainersDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddEstimateContainerSizeDuration(d time.Duration) {
	m.estimateContainerSizeDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddDeleteDuration(d time.Duration) {
	m.deleteDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddDropDuration(d time.Duration) {
	m.dropDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddExistsDuration(d time.Duration) {
	m.existsDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddGetDuration(d time.Duration) {
	m.getDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddHeadDuration(d time.Duration) {
	m.headDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddGetStreamDuration(d time.Duration) {
	m.getStreamDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddGetRangeStreamDuration(d time.Duration) {
	m.getRangeStreamDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddInhumeDuration(d time.Duration) {
	m.inhumeDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddPutDuration(d time.Duration) {
	m.putDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddRangeDuration(d time.Duration) {
	m.rangeDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddSearchDuration(d time.Duration) {
	m.searchDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddListObjectsDuration(d time.Duration) {
	m.listObjectsDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddGetECPartDuration(d time.Duration) {
	m.getECPartDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddGetECPartRangeDuration(d time.Duration) {
	m.getECPartRangeDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddHeadECPartDuration(d time.Duration) {
	m.headECPartDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddToContainerSize(cnrID string, size int64) {
	m.containerSize.With(
		prometheus.Labels{
			containerIDLabelKey: cnrID,
		},
	).Add(float64(size))
}

func (m engineMetrics) AddToPayloadCounter(shardID string, size int64) {
	m.payloadSize.With(prometheus.Labels{shardIDLabelKey: shardID}).Add(float64(size))
}

func (m engineMetrics) SetCapacitySize(shardID string, capacity uint64) {
	m.capacitySize.With(prometheus.Labels{shardIDLabelKey: shardID}).Set(float64(capacity))
}
