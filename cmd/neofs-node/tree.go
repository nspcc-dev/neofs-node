package main

import (
	"context"
	"errors"

	treeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/tree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
	"go.uber.org/zap"
)

func initTreeService(c *cfg) {
	treeConfig := treeconfig.Tree(c.appCfg)
	if !treeConfig.Enabled() {
		c.log.Info("tree service is not enabled, skip initialization")
		return
	}

	c.treeService = tree.New(
		tree.WithContainerSource(c.cfgObject.cnrSource),
		tree.WithEACLSource(c.cfgObject.eaclSource),
		tree.WithNetmapSource(c.netMapSource),
		tree.WithPrivateKey(&c.key.PrivateKey),
		tree.WithLogger(c.log),
		tree.WithStorage(c.cfgObject.cfgLocalStorage.localStorage),
		tree.WithContainerCacheSize(treeConfig.CacheSize()),
		tree.WithReplicationChannelCapacity(treeConfig.ReplicationChannelCapacity()),
		tree.WithReplicationWorkerCount(treeConfig.ReplicationWorkerCount()))

	for _, srv := range c.cfgGRPC.servers {
		tree.RegisterTreeServiceServer(srv, c.treeService)
	}

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		c.treeService.Start(ctx)
	}))

	subscribeToContainerRemoval(c, func(e event.Event) {
		ev := e.(containerEvent.DeleteSuccess)

		// This is executed asynchronously, so we don't care about the operation taking some time.
		err := c.treeService.DropTree(context.Background(), ev.ID, "")
		if err != nil && !errors.Is(err, pilorama.ErrTreeNotFound) {
			// Ignore pilorama.ErrTreeNotFound but other errors, including shard.ErrReadOnly, should be logged.
			c.log.Error("container removal event received, but trees weren't removed",
				zap.Stringer("cid", ev.ID),
				zap.String("error", err.Error()))
		}
	})

	c.onShutdown(c.treeService.Shutdown)
}
