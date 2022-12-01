package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type (
	engineMetrics struct {
		listContainersDuration        prometheus.Counter
		estimateContainerSizeDuration prometheus.Counter
		deleteDuration                prometheus.Counter
		existsDuration                prometheus.Counter
		getDuration                   prometheus.Counter
		headDuration                  prometheus.Counter
		inhumeDuration                prometheus.Counter
		putDuration                   prometheus.Counter
		rangeDuration                 prometheus.Counter
		searchDuration                prometheus.Counter
		listObjectsDuration           prometheus.Counter
		containerSize                 prometheus.GaugeVec
	}
)

const engineSubsystem = "engine"

func newEngineMetrics() engineMetrics {
	var (
		listContainersDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "list_containers_duration",
			Help:      "Accumulated duration of engine list containers operations",
		})

		estimateContainerSizeDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "estimate_container_size_duration",
			Help:      "Accumulated duration of engine container size estimate operations",
		})

		deleteDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "delete_duration",
			Help:      "Accumulated duration of engine delete operations",
		})

		existsDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "exists_duration",
			Help:      "Accumulated duration of engine exists operations",
		})

		getDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "get_duration",
			Help:      "Accumulated duration of engine get operations",
		})

		headDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "head_duration",
			Help:      "Accumulated duration of engine head operations",
		})

		inhumeDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "inhume_duration",
			Help:      "Accumulated duration of engine inhume operations",
		})

		putDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "put_duration",
			Help:      "Accumulated duration of engine put operations",
		})

		rangeDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "range_duration",
			Help:      "Accumulated duration of engine range operations",
		})

		searchDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "search_duration",
			Help:      "Accumulated duration of engine search operations",
		})

		listObjectsDuration = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "list_objects_duration",
			Help:      "Accumulated duration of engine list objects operations",
		})

		containerSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: engineSubsystem,
			Name:      "container_size",
			Help:      "Accumulated size of all objects in a container",
		}, []string{containerIDLabelKey})
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
}

func (m engineMetrics) AddListContainersDuration(d time.Duration) {
	m.listObjectsDuration.Add(float64(d))
}

func (m engineMetrics) AddEstimateContainerSizeDuration(d time.Duration) {
	m.estimateContainerSizeDuration.Add(float64(d))
}

func (m engineMetrics) AddDeleteDuration(d time.Duration) {
	m.deleteDuration.Add(float64(d))
}

func (m engineMetrics) AddExistsDuration(d time.Duration) {
	m.existsDuration.Add(float64(d))
}

func (m engineMetrics) AddGetDuration(d time.Duration) {
	m.getDuration.Add(float64(d))
}

func (m engineMetrics) AddHeadDuration(d time.Duration) {
	m.headDuration.Add(float64(d))
}

func (m engineMetrics) AddInhumeDuration(d time.Duration) {
	m.inhumeDuration.Add(float64(d))
}

func (m engineMetrics) AddPutDuration(d time.Duration) {
	m.putDuration.Add(float64(d))
}

func (m engineMetrics) AddRangeDuration(d time.Duration) {
	m.rangeDuration.Add(float64(d))
}

func (m engineMetrics) AddSearchDuration(d time.Duration) {
	m.searchDuration.Add(float64(d))
}

func (m engineMetrics) AddListObjectsDuration(d time.Duration) {
	m.listObjectsDuration.Add(float64(d))
}

func (m engineMetrics) AddToContainerSize(cnrID string, size int64) {
	m.containerSize.With(prometheus.Labels{containerIDLabelKey: cnrID}).Add(float64(size))
}
