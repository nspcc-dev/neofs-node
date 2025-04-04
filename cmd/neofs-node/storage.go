package main

import (
	"time"

	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	containerEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/panjf2000/ants/v2"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

func initLocalStorage(c *cfg) {
	ls := engine.New([]engine.Option{
		engine.WithShardPoolSize(c.engine.shardPoolSize),
		engine.WithErrorThreshold(c.engine.errorThreshold),
		engine.WithLogger(c.log),
		engine.WithIgnoreUninitedShards(c.engine.isIgnoreUninitedShards),
		engine.WithObjectPutRetryTimeout(c.engine.objectPutRetryDeadline),
		engine.WithContainersSource(c.cnrSrc),
		engine.WithMetrics(c.metricsCollector),
	}...)

	addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
		ls.HandleNewEpoch(ev.(netmap.NewEpoch).EpochNumber())
	})

	subscribeToContainerRemoval(c, func(e event.Event) {
		ev := e.(containerEvent.DeleteSuccess)

		err := ls.InhumeContainer(ev.ID)
		if err != nil {
			c.log.Warn("inhuming container after a chain event",
				zap.Stringer("cID", ev.ID), zap.Error(err))
		}
	})

	// allocate memory for the service;
	// service will be created later
	c.cfgObject.getSvc = new(getsvc.Service)

	var shardsAttached int
	for _, optsWithMeta := range c.shardOpts() {
		id, err := ls.AddShard(optsWithMeta.shOpts...)
		if err != nil {
			c.log.Error("failed to attach shard to engine", zap.Error(err))
		} else {
			shardsAttached++
			c.log.Info("shard attached to engine", zap.Stringer("id", id))
		}
	}
	if shardsAttached == 0 {
		fatalOnErr(engineconfig.ErrNoShardConfigured)
	}

	c.cfgObject.cfgLocalStorage.localStorage = ls

	c.onShutdown(func() {
		c.log.Info("closing components of the storage engine...")

		err := ls.Close()
		if err != nil {
			c.log.Info("storage engine closing failure",
				zap.Error(err),
			)
		} else {
			c.log.Info("all components of the storage engine closed successfully")
		}
	})
}

type shardOptsWithID struct {
	configID string
	shOpts   []shard.Option
}

func (c *cfg) shardOpts() []shardOptsWithID {
	shards := make([]shardOptsWithID, 0, len(c.engine.shards))

	for _, shCfg := range c.engine.shards {
		var writeCacheOpts []writecache.Option
		if wcRead := shCfg.WritecacheCfg; wcRead.Enabled {
			writeCacheOpts = append(writeCacheOpts,
				writecache.WithPath(wcRead.Path),
				writecache.WithMaxObjectSize(wcRead.MaxObjSize),
				writecache.WithMaxCacheSize(wcRead.SizeLimit),
				writecache.WithNoSync(wcRead.NoSync),
				writecache.WithLogger(c.log),
			)
		}

		var ss []blobstor.SubStorage
		for _, sRead := range shCfg.SubStorages {
			switch sRead.Typ {
			case fstree.Type:
				ss = append(ss, blobstor.SubStorage{
					Storage: fstree.New(
						fstree.WithPath(sRead.Path),
						fstree.WithPerm(sRead.Perm),
						fstree.WithDepth(sRead.Depth),
						fstree.WithNoSync(sRead.NoSync),
						fstree.WithCombinedCountLimit(sRead.CombinedCountLimit),
						fstree.WithCombinedSizeLimit(sRead.CombinedSizeLimit),
						fstree.WithCombinedSizeThreshold(sRead.CombinedSizeThreshold),
						fstree.WithCombinedWriteInterval(sRead.FlushInterval)),
					Policy: func(_ *objectSDK.Object, data []byte) bool {
						return true
					},
				})
			case peapod.Type:
				ss = append(ss, blobstor.SubStorage{
					Storage: peapod.New(sRead.Path, sRead.Perm, sRead.FlushInterval),
					Policy: func(_ *objectSDK.Object, data []byte) bool {
						return uint64(len(data)) < shCfg.SmallSizeObjectLimit
					},
				})
			default:
				// should never happen, that has already
				// been handled: when the config was read
			}
		}

		var sh shardOptsWithID
		sh.configID = shCfg.ID()
		sh.shOpts = []shard.Option{
			shard.WithLogger(c.log),
			shard.WithResyncMetabase(shCfg.ResyncMetabase),
			shard.WithMode(shCfg.Mode),
			shard.WithBlobStorOptions(
				blobstor.WithCompressObjects(shCfg.Compress),
				blobstor.WithUncompressableContentTypes(shCfg.UncompressableContentType),
				blobstor.WithStorages(ss),

				blobstor.WithLogger(c.log),
			),
			shard.WithMetaBaseOptions(
				meta.WithPath(shCfg.MetaCfg.Path),
				meta.WithPermissions(shCfg.MetaCfg.Perm),
				meta.WithMaxBatchSize(shCfg.MetaCfg.MaxBatchSize),
				meta.WithMaxBatchDelay(shCfg.MetaCfg.MaxBatchDelay),
				meta.WithBoltDBOptions(&bbolt.Options{
					Timeout: time.Second,
				}),

				meta.WithLogger(c.log),
				meta.WithEpochState(c.cfgNetmap.state),
				meta.WithContainers(containerPresenceChecker{src: c.cnrSrc}),
				meta.WithInitContext(c.ctx),
			),
			shard.WithWriteCache(shCfg.WritecacheCfg.Enabled),
			shard.WithWriteCacheOptions(writeCacheOpts...),
			shard.WithRemoverBatchSize(shCfg.GcCfg.RemoverBatchSize),
			shard.WithGCRemoverSleepInterval(shCfg.GcCfg.RemoverSleepInterval),
			shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
				pool, err := ants.NewPool(sz)
				fatalOnErr(err)

				return pool
			}),
		}

		shards = append(shards, sh)
	}

	return shards
}
