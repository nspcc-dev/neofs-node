package main

import (
	"context"
	"net"

	treeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/tree"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
	"google.golang.org/grpc"
)

func initTreeService(c *cfg) {
	endpoint := treeconfig.GRPC(c.appCfg).Endpoint()
	if endpoint == treeconfig.GRPCEndpointDefault {
		return
	}

	treeSvc := tree.New(
		tree.WithContainerSource(c.cfgObject.cnrSource),
		tree.WithNetmapSource(c.netMapSource),
		tree.WithPrivateKey(&c.key.PrivateKey),
		tree.WithLogger(c.log),
		tree.WithStorage(c.cfgObject.cfgLocalStorage.localStorage))

	treeServer := grpc.NewServer()

	c.onShutdown(func() {
		stopGRPC("NeoFS Tree Service API", treeServer, c.log)
		treeSvc.Shutdown()
	})

	lis, err := net.Listen("tcp", endpoint)
	fatalOnErr(err)

	tree.RegisterTreeServiceServer(treeServer, treeSvc)

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		treeSvc.Start(ctx)
		fatalOnErr(treeServer.Serve(lis))
	}))
}
