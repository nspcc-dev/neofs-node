package metrics

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

const (
	locationLabel = "location"
	countryLabel  = "country"
	cityLabel     = "city"

	containerLabel = "cid"
)

var (
	objectsCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "neofs",
		Name:      "count_objects_on_node",
		Help:      "Number of objects stored on this node",
	}, []string{locationLabel, countryLabel, cityLabel})

	spaceCounter = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "neofs",
		Name:      "container_space_sizes",
		Help:      "Space allocated by ContainerID",
	}, []string{containerLabel})

	counter = atomic.NewFloat64(0)
)

func init() {
	prometheus.MustRegister(
		objectsCount,
		spaceCounter,
	)
}

func spaceUpdater(m *syncStore) func() {
	return func() {
		m.mutex.RLock()
		for cid := range m.items {
			spaceCounter.
				With(prometheus.Labels{
					containerLabel: cid.String(),
				}).
				Set(float64(m.items[cid]))
		}
		m.mutex.RUnlock()
	}
}

func metricsUpdater(opts []string) func() {
	var (
		locationCode string
		countryCode  string
		cityCode     string
	)

	for i := range opts {
		ss := strings.Split(opts[i], "/")
		for j := range ss {
			switch s := strings.SplitN(ss[j], ":", 2); strings.ToLower(s[0]) {
			case locationLabel:
				locationCode = s[1]
			case countryLabel:
				countryCode = s[1]
			case cityLabel:
				cityCode = s[1]
			}
		}
	}

	return func() {
		objectsCount.With(prometheus.Labels{
			locationLabel: locationCode,
			countryLabel:  countryCode,
			cityLabel:     cityCode,
		}).Set(counter.Load())
	}
}
