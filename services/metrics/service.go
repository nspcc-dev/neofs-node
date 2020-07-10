package metrics

import (
	"context"

	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/metrics"
	"github.com/nspcc-dev/neofs-node/modules/grpc"
	"go.uber.org/zap"
)

type (
	// Service is an interface of the server of Metrics service.
	Service interface {
		MetricsServer
		grpc.Service
	}

	// Params groups the parameters of Metrics service server's constructor.
	Params struct {
		Logger    *zap.Logger
		Collector metrics.Collector
	}

	serviceMetrics struct {
		log *zap.Logger
		col metrics.Collector
	}
)

const (
	errEmptyLogger    = internal.Error("empty logger")
	errEmptyCollector = internal.Error("empty metrics collector")
)

// New is a Metrics service server's constructor.
func New(p Params) (Service, error) {
	switch {
	case p.Logger == nil:
		return nil, errEmptyLogger
	case p.Collector == nil:
		return nil, errEmptyCollector
	}

	return &serviceMetrics{
		log: p.Logger,
		col: p.Collector,
	}, nil
}

func (s *serviceMetrics) ResetSpaceCounter(_ context.Context, _ *ResetSpaceRequest) (*ResetSpaceResponse, error) {
	s.col.UpdateSpaceUsage()
	return &ResetSpaceResponse{}, nil
}

func (s *serviceMetrics) Name() string { return "metrics" }

func (s *serviceMetrics) Register(srv *grpc.Server) {
	RegisterMetricsServer(srv, s)
}
