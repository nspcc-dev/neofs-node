package metrics

import "github.com/prometheus/client_golang/prometheus"

func registerVersionMetric(namespace string, version string) {
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "version",
		ConstLabels: prometheus.Labels{
			"version": version,
		},
	})

	prometheus.MustRegister(g)
	g.Set(1)
}
