package metrics

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/network/transport/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/metrics"
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

var (
	errEmptyLogger    = errors.New("empty logger")
	errEmptyCollector = errors.New("empty metrics collector")
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
