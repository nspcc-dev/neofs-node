package main

import (
	accountingGRPC "github.com/nspcc-dev/neofs-api-go/v2/accounting/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	accountingService "github.com/nspcc-dev/neofs-node/pkg/services/accounting"
)

func initAccountingService(c *cfg) {
	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	balanceMorphWrapper, err := balance.NewFromMorph(c.cfgMorph.client, c.shared.basics.balanceSH, 0)
	fatalOnErr(err)

	server := accountingService.New(&c.key.PrivateKey, c.networkState, balanceMorphWrapper)

	for _, srv := range c.cfgGRPC.servers {
		accountingGRPC.RegisterAccountingServiceServer(srv, server)
	}
}
