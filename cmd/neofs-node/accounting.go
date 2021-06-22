package main

import (
	accountingGRPC "github.com/nspcc-dev/neofs-api-go/v2/accounting/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	accountingTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/accounting/grpc"
	accountingService "github.com/nspcc-dev/neofs-node/pkg/services/accounting"
	accounting "github.com/nspcc-dev/neofs-node/pkg/services/accounting/morph"
)

func initAccountingService(c *cfg) {
	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	balanceMorphWrapper, err := wrapper.NewFromMorph(c.cfgMorph.client, c.cfgAccounting.scriptHash, 0)
	fatalOnErr(err)

	server := accountingTransportGRPC.New(
		accountingService.NewSignService(
			&c.key.PrivateKey,
			accountingService.NewResponseService(
				accountingService.NewExecutionService(
					accounting.NewExecutor(balanceMorphWrapper),
				),
				c.respSvc,
			),
		),
	)

	for _, srv := range c.cfgGRPC.servers {
		accountingGRPC.RegisterAccountingServiceServer(srv, server)
	}
}
