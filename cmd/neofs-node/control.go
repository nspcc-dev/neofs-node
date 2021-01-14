package main

import (
	"context"
	"encoding/hex"
	"net"

	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const (
	cfgCtrlSvcSection = "control"

	cfgCtrlSvcAllowedKeys = cfgCtrlSvcSection + ".permitted_keys"

	cfgCtrlSvcGRPCSection = cfgCtrlSvcSection + ".grpc"
	cfgCtrlGRPCEndpoint   = cfgCtrlSvcGRPCSection + ".endpoint"
)

func initControlService(c *cfg) {
	strKeys := c.viper.GetStringSlice(cfgCtrlSvcAllowedKeys)
	keys := make([][]byte, 0, len(strKeys)+1) // +1 for node key

	keys = append(keys, crypto.MarshalPublicKey(&c.key.PublicKey))

	for i := range strKeys {
		key, err := hex.DecodeString(strKeys[i])
		fatalOnErr(err)

		if crypto.UnmarshalPublicKey(key) == nil {
			fatalOnErr(errors.Errorf("invalid permitted key for Control service %s", strKeys[i]))
		}

		keys = append(keys, key)
	}

	ctlSvc := controlSvc.New(
		controlSvc.WithKey(c.key),
		controlSvc.WithAuthorizedKeys(keys),
		controlSvc.WithHealthChecker(c),
	)

	var (
		err      error
		lis      net.Listener
		endpoint = c.viper.GetString(cfgCtrlGRPCEndpoint)
	)

	if endpoint == "" || endpoint == c.viper.GetString(cfgListenAddress) {
		lis = c.cfgGRPC.listener
		c.cfgControlService.server = c.cfgGRPC.server
	} else {
		lis, err = net.Listen("tcp", endpoint)
		fatalOnErr(err)

		c.cfgControlService.server = grpc.NewServer()
	}

	control.RegisterControlServiceServer(c.cfgControlService.server, ctlSvc)

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		fatalOnErr(c.cfgControlService.server.Serve(lis))
	}))
}

func (c *cfg) setHealthStatus(st control.HealthStatus) {
	c.healthStatus.Store(int32(st))
}

func (c *cfg) HealthStatus() control.HealthStatus {
	return control.HealthStatus(c.healthStatus.Load())
}
