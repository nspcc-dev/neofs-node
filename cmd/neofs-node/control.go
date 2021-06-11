package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	crypto "github.com/nspcc-dev/neofs-crypto"
	controlconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/control"
	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"google.golang.org/grpc"
)

func initControlService(c *cfg) {
	strKeys := controlconfig.AuthorizedKeysString(c.appCfg)
	keys := make([][]byte, 0, len(strKeys)+1) // +1 for node key

	keys = append(keys, crypto.MarshalPublicKey(&c.key.PublicKey))

	for i := range strKeys {
		key, err := hex.DecodeString(strKeys[i])
		fatalOnErr(err)

		if crypto.UnmarshalPublicKey(key) == nil {
			fatalOnErr(fmt.Errorf("invalid permitted key for Control service %s", strKeys[i]))
		}

		keys = append(keys, key)
	}

	ctlSvc := controlSvc.New(
		controlSvc.WithKey(c.key),
		controlSvc.WithAuthorizedKeys(keys),
		controlSvc.WithHealthChecker(c),
		controlSvc.WithNetMapSource(c.cfgNetmap.wrapper),
		controlSvc.WithNodeState(c),
		controlSvc.WithDeletedObjectHandler(func(addrList []*object.Address) error {
			prm := new(engine.DeletePrm).WithAddresses(addrList...)

			_, err := c.cfgObject.cfgLocalStorage.localStorage.Delete(prm)

			return err
		}),
	)

	var (
		err      error
		lis      net.Listener
		endpoint = controlconfig.GRPC(c.appCfg).Endpoint()
	)

	if endpoint == "" || endpoint == grpcconfig.Endpoint(c.appCfg) {
		lis = c.cfgGRPC.listener
		c.cfgControlService.server = c.cfgGRPC.server
	} else {
		lis, err = net.Listen("tcp", endpoint)
		fatalOnErr(err)

		c.cfgControlService.server = grpc.NewServer()
	}

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
