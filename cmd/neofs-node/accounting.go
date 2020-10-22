package main

import (
	accountingGRPC "github.com/nspcc-dev/neofs-api-go/v2/accounting/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	accountingTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/accounting/grpc"
	accountingService "github.com/nspcc-dev/neofs-node/pkg/services/accounting"
	accounting "github.com/nspcc-dev/neofs-node/pkg/services/accounting/morph"
)

func initAccountingService(c *cfg) {
	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	staticClient, err := client.NewStatic(
		c.cfgMorph.client,
		c.cfgAccounting.scriptHash,
		c.cfgAccounting.fee,
	)
	fatalOnErr(err)

	balanceClient, err := balance.New(staticClient)
	fatalOnErr(err)

	balanceMorphWrapper, err := wrapper.New(balanceClient)
	fatalOnErr(err)

	accountingGRPC.RegisterAccountingServiceServer(c.cfgGRPC.server,
		accountingTransportGRPC.New(
			accountingService.NewSignService(
				c.key,
				accountingService.NewResponseService(
					accountingService.NewExecutionService(
						accounting.NewExecutor(balanceMorphWrapper),
						new(session.ResponseMetaHeader),
					),
					c.respSvc,
				),
			),
		),
	)
}
