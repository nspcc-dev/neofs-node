package node

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/grpc"
	accounting "github.com/nspcc-dev/neofs-node/pkg/network/transport/accounting/grpc"
	container "github.com/nspcc-dev/neofs-node/pkg/network/transport/container/grpc"
	metrics "github.com/nspcc-dev/neofs-node/pkg/network/transport/metrics/grpc"
	object "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	session "github.com/nspcc-dev/neofs-node/pkg/network/transport/session/grpc"
	state "github.com/nspcc-dev/neofs-node/pkg/network/transport/state/grpc"
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
