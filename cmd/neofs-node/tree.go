package main

import (
	"context"
	"errors"
	"time"

	treeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/tree"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	containerClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.uber.org/zap"
)

type cnrSource struct {
	// cache of raw client.
	src container.Source
	// raw client; no need to cache request results
	// since sync is performed once in epoch and is
	// expected to receive different results every
	// call.
	cli *containerClient.Client
}

func (c cnrSource) Get(id cid.ID) (*container.Container, error) {
	return c.src.Get(id)
}

func (c cnrSource) List() ([]cid.ID, error) {
	return c.cli.List(nil)
}

func initTreeService(c *cfg) {
	treeConfig := treeconfig.Tree(c.cfgReader)
	if !treeConfig.Enabled() {
		c.log.Info("tree service is not enabled, skip initialization")
		return
	}

	c.treeService = tree.New(
		tree.WithContainerSource(cnrSource{
			src: c.cnrSrc,
			cli: c.shared.basics.cCli,
		}),
		tree.WithEACLSource(c.eaclSrc),
		tree.WithNetmapSource(c.netMapSource),
		tree.WithPrivateKey(&c.key.PrivateKey),
		tree.WithLogger(c.log),
		tree.WithStorage(c.cfgObject.cfgLocalStorage.localStorage),
		tree.WithContainerCacheSize(treeConfig.CacheSize()),
		tree.WithReplicationTimeout(treeConfig.ReplicationTimeout()),
		tree.WithReplicationChannelCapacity(treeConfig.ReplicationChannelCapacity()),
		tree.WithReplicationWorkerCount(treeConfig.ReplicationWorkerCount()))

	for _, srv := range c.cfgGRPC.servers {
		tree.RegisterTreeServiceServer(srv, c.treeService)
	}

	c.workers = append(c.workers, newWorkerFromFunc(func(ctx context.Context) {
		c.treeService.Start(ctx)
	}))

	if d := treeConfig.SyncInterval(); d == 0 {
		addNewEpochNotificationHandler(c, func(_ event.Event) {
			err := c.treeService.SynchronizeAll()
			if err != nil {
				c.log.Error("could not synchronize Tree Service", zap.Error(err))
			}
		})
	} else {
		go func() {
			tick := time.NewTicker(d)

			for range tick.C {
				err := c.treeService.SynchronizeAll()
				if err != nil {
					c.log.Error("could not synchronize Tree Service", zap.Error(err))
					if errors.Is(err, tree.ErrShuttingDown) {
						return
					}
				}
			}
		}()
	}

	subscribeToContainerRemoval(c, func(e event.Event) {
		ev := e.(containerEvent.DeleteSuccess)

		// This is executed asynchronously, so we don't care about the operation taking some time.
		c.log.Debug("removing all trees for container", zap.Stringer("cid", ev.ID))
		err := c.treeService.DropTree(context.Background(), ev.ID, "")
		if err != nil && !errors.Is(err, pilorama.ErrTreeNotFound) {
			// Ignore pilorama.ErrTreeNotFound but other errors, including shard.ErrReadOnly, should be logged.
			c.log.Error("container removal event received, but trees weren't removed",
				zap.Stringer("cid", ev.ID),
				zap.Error(err))
		}
	})

	c.onShutdown(c.treeService.Shutdown)
}
