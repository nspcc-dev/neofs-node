package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type (
	engineMetrics struct {
		listContainersDurationCounter        prometheus.Counter
		estimateContainerSizeDurationCounter prometheus.Counter
		deleteDurationCounter                prometheus.Counter
		existsDurationCounter                prometheus.Counter
		getDurationCounter                   prometheus.Counter
		headDurationCounter                  prometheus.Counter
		inhumeDurationCounter                prometheus.Counter
		putDurationCounter                   prometheus.Counter
		rangeDurationCounter                 prometheus.Counter
		searchDurationCounter                prometheus.Counter
		listObjectsDurationCounter           prometheus.Counter

		listContainersDuration        prometheus.Histogram
		estimateContainerSizeDuration prometheus.Histogram
		deleteDuration                prometheus.Histogram
		existsDuration                prometheus.Histogram
		getDuration                   prometheus.Histogram
		headDuration                  prometheus.Histogram
		inhumeDuration                prometheus.Histogram
		putDuration                   prometheus.Histogram
		rangeDuration                 prometheus.Histogram
		searchDuration                prometheus.Histogram
		listObjectsDuration           prometheus.Histogram

		containerSize prometheus.GaugeVec
		payloadSize   prometheus.GaugeVec
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
	)

	var (
		listContainersDurationCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "list_containers_duration",
			Help:      "Accumulated duration of engine 'list containers' operations [DEPRECATED]",
		})

		estimateContainerSizeDurationCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "estimate_container_size_duration",
			Help:      "Accumulated duration of engine 'container size estimate' operations [DEPRECATED]",
		})

		deleteDurationCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "delete_duration",
			Help:      "Accumulated duration of engine 'delete' operations [DEPRECATED]",
		})

		existsDurationCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "exists_duration",
			Help:      "Accumulated duration of engine 'exists' operations [DEPRECATED]",
		})

		getDurationCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "get_duration",
			Help:      "Accumulated duration of engine 'get' operations [DEPRECATED]",
		})

		headDurationCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "head_duration",
			Help:      "Accumulated duration of engine 'head' operations [DEPRECATED]",
		})

		inhumeDurationCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "inhume_duration",
			Help:      "Accumulated duration of engine 'inhume' operations [DEPRECATED]",
		})

		putDurationCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "put_duration",
			Help:      "Accumulated duration of engine 'put' operations [DEPRECATED]",
		})

		rangeDurationCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "range_duration",
			Help:      "Accumulated duration of engine 'range' operations [DEPRECATED]",
		})

		searchDurationCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "search_duration",
			Help:      "Accumulated duration of engine 'search' operations [DEPRECATED]",
		})

		listObjectsDurationCounter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: storageNodeNameSpace,
			Subsystem: engineSubsystem,
			Name:      "list_objects_duration",
			Help:      "Accumulated duration of engine 'list objects' operations [DEPRECATED]",
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
	)

	return engineMetrics{
		listContainersDuration:        listContainersDuration,
		estimateContainerSizeDuration: estimateContainerSizeDuration,
		deleteDuration:                deleteDuration,
		existsDuration:                existsDuration,
		getDuration:                   getDuration,
		headDuration:                  headDuration,
		inhumeDuration:                inhumeDuration,
		putDuration:                   putDuration,
		rangeDuration:                 rangeDuration,
		searchDuration:                searchDuration,
		listObjectsDuration:           listObjectsDuration,
		containerSize:                 *containerSize,
		payloadSize:                   *payloadSize,

		listContainersDurationCounter:        listContainersDurationCounter,
		estimateContainerSizeDurationCounter: estimateContainerSizeDurationCounter,
		deleteDurationCounter:                deleteDurationCounter,
		existsDurationCounter:                existsDurationCounter,
		getDurationCounter:                   getDurationCounter,
		headDurationCounter:                  headDurationCounter,
		inhumeDurationCounter:                inhumeDurationCounter,
		putDurationCounter:                   putDurationCounter,
		rangeDurationCounter:                 rangeDurationCounter,
		searchDurationCounter:                searchDurationCounter,
		listObjectsDurationCounter:           listObjectsDurationCounter,
	}
}

func (m engineMetrics) register() {
	prometheus.MustRegister(m.listContainersDuration)
	prometheus.MustRegister(m.estimateContainerSizeDuration)
	prometheus.MustRegister(m.deleteDuration)
	prometheus.MustRegister(m.existsDuration)
	prometheus.MustRegister(m.getDuration)
	prometheus.MustRegister(m.headDuration)
	prometheus.MustRegister(m.inhumeDuration)
	prometheus.MustRegister(m.putDuration)
	prometheus.MustRegister(m.rangeDuration)
	prometheus.MustRegister(m.searchDuration)
	prometheus.MustRegister(m.listObjectsDuration)
	prometheus.MustRegister(m.containerSize)
	prometheus.MustRegister(m.payloadSize)

	prometheus.MustRegister(m.listContainersDurationCounter)
	prometheus.MustRegister(m.estimateContainerSizeDurationCounter)
	prometheus.MustRegister(m.deleteDurationCounter)
	prometheus.MustRegister(m.existsDurationCounter)
	prometheus.MustRegister(m.getDurationCounter)
	prometheus.MustRegister(m.headDurationCounter)
	prometheus.MustRegister(m.inhumeDurationCounter)
	prometheus.MustRegister(m.putDurationCounter)
	prometheus.MustRegister(m.rangeDurationCounter)
	prometheus.MustRegister(m.searchDurationCounter)
	prometheus.MustRegister(m.listObjectsDurationCounter)
}

func (m engineMetrics) AddListContainersDuration(d time.Duration) {
	m.listObjectsDurationCounter.Add(float64(d))
	m.listObjectsDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddEstimateContainerSizeDuration(d time.Duration) {
	m.estimateContainerSizeDurationCounter.Add(float64(d))
	m.estimateContainerSizeDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddDeleteDuration(d time.Duration) {
	m.deleteDurationCounter.Add(float64(d))
	m.deleteDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddExistsDuration(d time.Duration) {
	m.existsDurationCounter.Add(float64(d))
	m.existsDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddGetDuration(d time.Duration) {
	m.getDurationCounter.Add(float64(d))
	m.getDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddHeadDuration(d time.Duration) {
	m.headDurationCounter.Add(float64(d))
	m.headDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddInhumeDuration(d time.Duration) {
	m.inhumeDurationCounter.Add(float64(d))
	m.inhumeDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddPutDuration(d time.Duration) {
	m.putDurationCounter.Add(float64(d))
	m.putDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddRangeDuration(d time.Duration) {
	m.rangeDurationCounter.Add(float64(d))
	m.rangeDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddSearchDuration(d time.Duration) {
	m.searchDurationCounter.Add(float64(d))
	m.searchDuration.Observe(d.Seconds())
}

func (m engineMetrics) AddListObjectsDuration(d time.Duration) {
	m.listObjectsDurationCounter.Add(float64(d))
	m.listObjectsDuration.Observe(d.Seconds())
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
