package main

import (
	accountingService "github.com/nspcc-dev/neofs-node/pkg/services/accounting"
	protoaccounting "github.com/nspcc-dev/neofs-sdk-go/proto/accounting"
	"google.golang.org/grpc"
)

func initAccountingService(c *cfg) {
	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	server := accountingService.New(&c.key.PrivateKey, c.networkState, c.bCli)

	c.cfgGRPC.registerService(func(srv *grpc.Server) {
		protoaccounting.RegisterAccountingServiceServer(srv, server)
	})
}
