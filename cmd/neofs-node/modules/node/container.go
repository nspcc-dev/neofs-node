package node

import (
	svc "github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/bootstrap"
	eacl "github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended/storage"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/storage"
	container "github.com/nspcc-dev/neofs-node/pkg/network/transport/container/grpc"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type cnrParams struct {
	dig.In

	Logger *zap.Logger

	Healthy svc.HealthyClient

	ExtendedACLStore eacl.Storage

	ContainerStorage storage.Storage
}

func newContainerService(p cnrParams) (container.Service, error) {
	return container.New(container.Params{
		Logger:           p.Logger,
		Healthy:          p.Healthy,
		Store:            p.ContainerStorage,
		ExtendedACLStore: p.ExtendedACLStore,
	})
}
