package main

import (
	"context"
	"encoding/hex"
	"net"

	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/services/private"
	privateSvc "github.com/nspcc-dev/neofs-node/pkg/services/private/server"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const (
	cfgPrivateSvcSection = "private"

	cfgPrivateSvcAllowedKeys = cfgPrivateSvcSection + ".permitted_keys"

	cfgPrivateSvcGRPCSection = cfgPrivateSvcSection + ".grpc"
	cfgPrivateGRPCEndpoint   = cfgPrivateSvcGRPCSection + ".endpoint"
)

func initPrivateService(c *cfg) {
	strKeys := c.viper.GetStringSlice(cfgPrivateSvcAllowedKeys)
	keys := make([][]byte, 0, len(strKeys)+1) // +1 for node key

	keys = append(keys, crypto.MarshalPublicKey(&c.key.PublicKey))

	for i := range strKeys {
		key, err := hex.DecodeString(strKeys[i])
		fatalOnErr(err)

		if crypto.UnmarshalPublicKey(key) == nil {
			fatalOnErr(errors.Errorf("invalid permitted key for private service %s", strKeys[i]))
		}

		keys = append(keys, key)
	}

	privSvc := privateSvc.New(
		privateSvc.WithKey(c.key),
		privateSvc.WithAllowedKeys(keys),
		privateSvc.WithHealthChecker(c),
	)

	var (
		err      error
		lis      net.Listener
		endpoint = c.viper.GetString(cfgPrivateGRPCEndpoint)
	)

	if endpoint == "" || endpoint == c.viper.GetString(cfgListenAddress) {
		lis = c.cfgGRPC.listener
		c.cfgPrivateService.server = c.cfgGRPC.server
	} else {
		lis, err = net.Listen("tcp", endpoint)
		fatalOnErr(err)

		c.cfgPrivateService.server = grpc.NewServer()
	}

	private.RegisterPrivateServiceServer(c.cfgPrivateService.server, privSvc)

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		fatalOnErr(c.cfgPrivateService.server.Serve(lis))
	}))
}

func (c *cfg) setHealthStatus(st private.HealthStatus) {
	c.healthStatus.Store(int32(st))
}

func (c *cfg) HealthStatus() private.HealthStatus {
	return private.HealthStatus(c.healthStatus.Load())
}
