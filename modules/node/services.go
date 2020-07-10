package node

import (
	"github.com/nspcc-dev/neofs-node/modules/grpc"
	"github.com/nspcc-dev/neofs-node/services/metrics"
	"github.com/nspcc-dev/neofs-node/services/public/accounting"
	"github.com/nspcc-dev/neofs-node/services/public/container"
	"github.com/nspcc-dev/neofs-node/services/public/object"
	"github.com/nspcc-dev/neofs-node/services/public/session"
	"github.com/nspcc-dev/neofs-node/services/public/state"
	"go.uber.org/dig"
)

type servicesParams struct {
	dig.In

	Status     state.Service
	Container  container.Service
	Object     object.Service
	Session    session.Service
	Accounting accounting.Service
	Metrics    metrics.Service
}

func attachServices(p servicesParams) grpc.ServicesResult {
	return grpc.ServicesResult{
		Services: []grpc.Service{
			p.Status,
			p.Container,
			p.Accounting,
			p.Metrics,
			p.Session,
			p.Object,
		},
	}
}
