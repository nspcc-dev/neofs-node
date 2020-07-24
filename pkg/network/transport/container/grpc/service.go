package container

import (
	"errors"

	"github.com/nspcc-dev/neofs-api-go/container"
	eacl "github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended/storage"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/storage"
	"github.com/nspcc-dev/neofs-node/pkg/network/transport/grpc"
	libgrpc "github.com/nspcc-dev/neofs-node/pkg/network/transport/grpc"
	"go.uber.org/zap"
)

type (
	// Service is an interface of the server of Container service.
	Service interface {
		grpc.Service
		container.ServiceServer
	}

	// HealthChecker is an interface of node healthiness checking tool.
	HealthChecker interface {
		Healthy() error
	}

	// Params groups the parameters of Container service server's constructor.
	Params struct {
		Logger *zap.Logger

		Healthy HealthChecker

		Store storage.Storage

		ExtendedACLStore eacl.Storage
	}

	cnrService struct {
		log *zap.Logger

		healthy HealthChecker

		cnrStore storage.Storage

		aclStore eacl.Storage
	}
)

var (
	errEmptyLogger        = errors.New("empty log component")
	errEmptyHealthChecker = errors.New("empty healthy component")
)

var requestVerifyFunc = libgrpc.VerifyRequestWithSignatures

// New is an Container service server's constructor.
func New(p Params) (Service, error) {
	switch {
	case p.Logger == nil:
		return nil, errEmptyLogger
	case p.Store == nil:
		return nil, storage.ErrNilStorage
	case p.Healthy == nil:
		return nil, errEmptyHealthChecker
	case p.ExtendedACLStore == nil:
		return nil, eacl.ErrNilStorage
	}

	return &cnrService{
		log:      p.Logger,
		healthy:  p.Healthy,
		cnrStore: p.Store,
		aclStore: p.ExtendedACLStore,
	}, nil
}

func (cnrService) Name() string { return "ContainerService" }

func (s cnrService) Register(g *grpc.Server) { container.RegisterServiceServer(g, s) }
