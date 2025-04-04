package main

import (
	"net"

	controlconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/control"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func initControlService(c *cfg) {
	endpoint := controlconfig.GRPC(c.cfgReader).Endpoint()
	if endpoint == controlconfig.GRPCEndpointDefault {
		return
	}

	pubs := controlconfig.AuthorizedKeys(c.cfgReader)
	rawPubs := make([][]byte, 0, len(pubs)+1) // +1 for node key

	rawPubs = append(rawPubs, c.key.PublicKey().Bytes())

	for i := range pubs {
		rawPubs = append(rawPubs, pubs[i].Bytes())
	}

	c.shared.control = controlSvc.New(&c.key.PrivateKey, rawPubs, c)

	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		c.log.Error("can't listen gRPC endpoint (control)", zap.Error(err))
		return
	}

	c.cfgControlService.server = grpc.NewServer()

	c.onShutdown(func() {
		stopGRPC("NeoFS Control API", c.cfgControlService.server, c.log)
	})

	control.RegisterControlServiceServer(c.cfgControlService.server, c.shared.control)
	c.wg.Add(1)
	go func() {
		runAndLog(c, "control", false, func(c *cfg) {
			fatalOnErr(c.cfgControlService.server.Serve(lis))
			c.wg.Done()
		})
	}()
}

func (c *cfg) NetmapStatus() control.NetmapStatus {
	return c.cfgNetmap.state.controlNetmapStatus()
}

func (c *cfg) setHealthStatus(st control.HealthStatus) {
	c.healthStatus.Store(int32(st))

	c.metricsCollector.SetHealth(int32(st))
}

func (c *cfg) HealthStatus() control.HealthStatus {
	return control.HealthStatus(c.healthStatus.Load())
}
