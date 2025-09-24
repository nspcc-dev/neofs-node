package main

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	accountingService "github.com/nspcc-dev/neofs-node/pkg/services/accounting"
	protoaccounting "github.com/nspcc-dev/neofs-sdk-go/proto/accounting"
)

func initAccountingService(c *cfg) {
	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	balanceMorphWrapper, err := balance.NewFromMorph(c.cfgMorph.client, c.balanceSH)
	fatalOnErr(err)

	server := accountingService.New(&c.key.PrivateKey, c.networkState, balanceMorphWrapper)

	for _, srv := range c.cfgGRPC.servers {
		protoaccounting.RegisterAccountingServiceServer(srv, server)
	}
}
