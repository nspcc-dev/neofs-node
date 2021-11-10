package main

import (
	"context"
	"net"

	controlconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/control"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"google.golang.org/grpc"
)

func initControlService(c *cfg) {
	endpoint := controlconfig.GRPC(c.appCfg).Endpoint()
	if endpoint == controlconfig.GRPCEndpointDefault {
		return
	}

	pubs := controlconfig.AuthorizedKeys(c.appCfg)
	rawPubs := make([][]byte, 0, len(pubs)+1) // +1 for node key

	rawPubs = append(rawPubs, c.key.PublicKey().Bytes())

	for i := range pubs {
		rawPubs = append(rawPubs, pubs[i].Bytes())
	}

	ctlSvc := controlSvc.New(
		controlSvc.WithKey(&c.key.PrivateKey),
		controlSvc.WithAuthorizedKeys(rawPubs),
		controlSvc.WithHealthChecker(c),
		controlSvc.WithNetMapSource(c.cfgNetmap.wrapper),
		controlSvc.WithNodeState(c),
		controlSvc.WithDeletedObjectHandler(func(addrList []*object.Address) error {
			prm := new(engine.DeletePrm).WithAddresses(addrList...)

			_, err := c.cfgObject.cfgLocalStorage.localStorage.Delete(prm)

			return err
		}),
	)

	lis, err := net.Listen("tcp", endpoint)
	fatalOnErr(err)

	c.cfgControlService.server = grpc.NewServer()

	c.onShutdown(func() {
		stopGRPC("NeoFS Control API", c.cfgControlService.server, c.log)
	})

	control.RegisterControlServiceServer(c.cfgControlService.server, ctlSvc)

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		fatalOnErr(c.cfgControlService.server.Serve(lis))
	}))
}

func (c *cfg) NetmapStatus() control.NetmapStatus {
	return c.cfgNetmap.state.controlNetmapStatus()
}

func (c *cfg) setHealthStatus(st control.HealthStatus) {
	c.healthStatus.Store(int32(st))
}

func (c *cfg) HealthStatus() control.HealthStatus {
	return control.HealthStatus(c.healthStatus.Load())
}
