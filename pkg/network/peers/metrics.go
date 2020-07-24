package peers

import (
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/connectivity"
)

const stateLabel = "state"

var grpcConnections = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Help:      "gRPC connections",
		Name:      "grpc_connections",
		Namespace: "neofs",
	},
	[]string{stateLabel},
)

var conStates = []connectivity.State{
	connectivity.Idle,
	connectivity.Connecting,
	connectivity.Ready,
	connectivity.TransientFailure,
	connectivity.Shutdown,
}

func updateMetrics(items map[connectivity.State]float64) {
	for _, state := range conStates {
		grpcConnections.With(prometheus.Labels{
			stateLabel: state.String(),
		}).Set(items[state])
	}
}

func init() {
	prometheus.MustRegister(
		grpcConnections,
	)

	for _, state := range conStates {
		grpcConnections.With(prometheus.Labels{
			stateLabel: state.String(),
		}).Set(0)
	}
}
