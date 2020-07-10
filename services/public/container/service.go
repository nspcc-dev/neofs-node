package container

import (
	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/acl"
	libcnr "github.com/nspcc-dev/neofs-node/lib/container"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/modules/grpc"
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

		Store libcnr.Storage

		ExtendedACLStore acl.BinaryExtendedACLStore
	}

	cnrService struct {
		log *zap.Logger

		healthy HealthChecker

		cnrStore libcnr.Storage

		aclStore acl.BinaryExtendedACLStore
	}
)

const (
	errEmptyLogger        = internal.Error("empty log component")
	errEmptyStore         = internal.Error("empty store component")
	errEmptyHealthChecker = internal.Error("empty healthy component")
)

var requestVerifyFunc = core.VerifyRequestWithSignatures

// New is an Container service server's constructor.
func New(p Params) (Service, error) {
	switch {
	case p.Logger == nil:
		return nil, errEmptyLogger
	case p.Store == nil:
		return nil, errEmptyStore
	case p.Healthy == nil:
		return nil, errEmptyHealthChecker
	case p.ExtendedACLStore == nil:
		return nil, acl.ErrNilBinaryExtendedACLStore
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
