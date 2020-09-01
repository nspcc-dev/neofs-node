package main

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
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

	u160, err := util.Uint160DecodeStringLE(c.cfgAccounting.scriptHash)
	fatalOnErr(err)

	staticClient, err := client.NewStatic(c.cfgMorph.client, u160, c.cfgAccounting.fee)
	fatalOnErr(err)

	balanceClient, err := balance.New(staticClient)
	fatalOnErr(err)

	balanceMorphWrapper, err := wrapper.New(balanceClient)
	fatalOnErr(err)

	metaHdr := new(session.ResponseMetaHeader)
	xHdr := new(session.XHeader)
	xHdr.SetKey("test X-Header key")
	xHdr.SetValue("test X-Header value")
	metaHdr.SetXHeaders([]*session.XHeader{xHdr})

	accountingGRPC.RegisterAccountingServiceServer(c.cfgGRPC.server,
		accountingTransportGRPC.New(
			accountingService.NewSignService(
				c.key,
				accountingService.NewExecutionService(
					accounting.NewExecutor(balanceMorphWrapper),
					metaHdr,
				),
			),
		),
	)
}
