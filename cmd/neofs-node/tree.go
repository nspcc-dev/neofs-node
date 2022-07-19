package main

import (
	"context"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
)

func initTreeService(c *cfg) {
	sub := c.appCfg.Sub("tree")
	if !config.BoolSafe(sub, "enabled") {
		c.log.Info("tree service is not enabled, skip initialization")
		return
	}

	c.treeService = tree.New(
		tree.WithContainerSource(c.cfgObject.cnrSource),
		tree.WithNetmapSource(c.netMapSource),
		tree.WithPrivateKey(&c.key.PrivateKey),
		tree.WithLogger(c.log),
		tree.WithStorage(c.cfgObject.cfgLocalStorage.localStorage),
		tree.WithContainerCacheSize(int(config.IntSafe(sub, "cache_size"))),
		tree.WithReplicationChannelCapacity(int(config.IntSafe(sub, "replication_channel_capacity"))),
		tree.WithReplicationWorkerCount(int(config.IntSafe(sub, "replication_worker_count"))))

	for _, srv := range c.cfgGRPC.servers {
		tree.RegisterTreeServiceServer(srv, c.treeService)
	}

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		c.treeService.Start(ctx)
	}))

	c.onShutdown(c.treeService.Shutdown)
}
