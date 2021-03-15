package engine

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "neofs_node"
	subsystem = "engine"
)

var (
	listContainersDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "list_containers_duration",
		Help:      "Accumulated duration of engine list containers operations",
	})

	estimateContainerSizeDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "estimate_container_size_duration",
		Help:      "Accumulated duration of engine container size estimate operations",
	})

	deleteDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "delete_duration",
		Help:      "Accumulated duration of engine delete operations",
	})

	existsDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "exists_duration",
		Help:      "Accumulated duration of engine exists operations",
	})

	getDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "get_duration",
		Help:      "Accumulated duration of engine get operations",
	})

	headDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "head_duration",
		Help:      "Accumulated duration of engine head operations",
	})

	inhumeDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "inhume_duration",
		Help:      "Accumulated duration of engine inhume operations",
	})

	putDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "put_duration",
		Help:      "Accumulated duration of engine put operations",
	})

	rangeDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "range_duration",
		Help:      "Accumulated duration of engine range operations",
	})

	searchDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "search_duration",
		Help:      "Accumulated duration of engine search operations",
	})

	listObjectsDuration = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "list_objects_duration",
		Help:      "Accumulated duration of engine list objects operations",
	})
)

func registerMetrics() {
	prometheus.MustRegister(listContainersDuration)
	prometheus.MustRegister(estimateContainerSizeDuration)
	prometheus.MustRegister(deleteDuration)
	prometheus.MustRegister(existsDuration)
	prometheus.MustRegister(getDuration)
	prometheus.MustRegister(headDuration)
	prometheus.MustRegister(inhumeDuration)
	prometheus.MustRegister(putDuration)
	prometheus.MustRegister(rangeDuration)
	prometheus.MustRegister(searchDuration)
	prometheus.MustRegister(listObjectsDuration)
}

func elapsed(c prometheus.Counter) func() {
	t := time.Now()

	return func() {
		c.Add(float64(time.Since(t)))
	}
}
