package node

import (
	"github.com/nspcc-dev/neofs-node/lib/acl"
	libcnr "github.com/nspcc-dev/neofs-node/lib/container"
	svc "github.com/nspcc-dev/neofs-node/modules/bootstrap"
	"github.com/nspcc-dev/neofs-node/services/public/container"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type cnrParams struct {
	dig.In

	Logger *zap.Logger

	Healthy svc.HealthyClient

	ExtendedACLStore acl.BinaryExtendedACLStore

	ContainerStorage libcnr.Storage
}

func newContainerService(p cnrParams) (container.Service, error) {
	return container.New(container.Params{
		Logger:           p.Logger,
		Healthy:          p.Healthy,
		Store:            p.ContainerStorage,
		ExtendedACLStore: p.ExtendedACLStore,
	})
}
