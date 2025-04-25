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
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/panjf2000/ants/v2"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

func initLocalStorage(c *cfg) {
	ls := engine.New([]engine.Option{
		engine.WithShardPoolSize(uint32(c.appCfg.Storage.ShardPoolSize)),
		engine.WithErrorThreshold(uint32(c.appCfg.Storage.ShardROErrorThreshold)),
		engine.WithLogger(c.log),
		engine.WithIgnoreUninitedShards(c.appCfg.Storage.IgnoreUninitedShards),
		engine.WithObjectPutRetryTimeout(c.appCfg.Storage.PutRetryTimeout),
		engine.WithContainersSource(c.cnrSrc),
		engine.WithMetrics(c.metricsCollector),
	}...)

	addNewEpochAsyncNotificationHandler(c, func(ev event.Event) {
		ls.HandleNewEpoch(ev.(netmap.NewEpoch).EpochNumber())
	})

	subscribeToContainerRemoval(c, func(id cid.ID, owner user.ID) {
		if owner.IsZero() {
			err := ls.InhumeContainer(id)
			if err != nil {
				c.log.Warn("inhuming container after a chain event",
					zap.Stringer("cID", id), zap.Error(err))
			}
			return
		}
		c.log.Info("caught container removal, marking its local objects for GC...",
			zap.Stringer("container", id), zap.Stringer("owner", owner))

		if err := ls.InhumeContainer(id); err != nil {
			c.log.Warn("failed to mark local objects from the removed container for GC",
				zap.Stringer("container", id), zap.Error(err))
			return
		}

		c.log.Info("successfully marked local objects from the removed container for GC",
			zap.Stringer("container", id))
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
	shards := make([]shardOptsWithID, 0, len(c.appCfg.Storage.ShardList))

	for _, shCfg := range c.appCfg.Storage.ShardList {
		var (
			wcMaxBatchSize      uint64
			wcMaxBatchCount     int
			wcMaxBatchThreshold uint64
		)
		var ss blobstor.SubStorage
		sRead := shCfg.Blobstor
		switch sRead.Type {
		case fstree.Type:
			wcMaxBatchSize = uint64(sRead.CombinedSizeLimit)
			wcMaxBatchCount = sRead.CombinedCountLimit
			wcMaxBatchThreshold = uint64(sRead.CombinedSizeThreshold)
			ss = blobstor.SubStorage{
				Storage: fstree.New(
					fstree.WithPath(sRead.Path),
					fstree.WithPerm(sRead.Perm),
					fstree.WithDepth(sRead.Depth),
					fstree.WithNoSync(*sRead.NoSync),
					fstree.WithCombinedCountLimit(sRead.CombinedCountLimit),
					fstree.WithCombinedSizeLimit(int(sRead.CombinedSizeLimit)),
					fstree.WithCombinedSizeThreshold(int(sRead.CombinedSizeThreshold)),
					fstree.WithCombinedWriteInterval(sRead.FlushInterval)),
				Policy: func(_ *objectSDK.Object, data []byte) bool {
					return true
				},
			}
		case peapod.Type:
			ss = blobstor.SubStorage{
				Storage: peapod.New(sRead.Path, sRead.Perm, sRead.FlushInterval),
				Policy: func(_ *objectSDK.Object, data []byte) bool {
					return uint64(len(data)) < uint64(shCfg.SmallObjectSize)
				},
			}
		default:
			// should never happen, that has already
			// been handled: when the config was read
		}

		var writeCacheOpts []writecache.Option
		if wcRead := shCfg.WriteCache; *wcRead.Enabled {
			writeCacheOpts = append(writeCacheOpts,
				writecache.WithPath(wcRead.Path),
				writecache.WithMaxObjectSize(uint64(wcRead.MaxObjectSize)),
				writecache.WithMaxCacheSize(uint64(wcRead.Capacity)),
				writecache.WithNoSync(*wcRead.NoSync),
				writecache.WithLogger(c.log),
				writecache.WithMaxFlushBatchSize(wcMaxBatchSize),
				writecache.WithMaxFlushBatchCount(wcMaxBatchCount),
				writecache.WithMaxFlushBatchThreshold(wcMaxBatchThreshold),
				writecache.WithMetrics(c.metricsCollector),
			)
		}

		var sh shardOptsWithID
		sh.configID = shCfg.ID()
		sh.shOpts = []shard.Option{
			shard.WithLogger(c.log),
			shard.WithResyncMetabase(*shCfg.ResyncMetabase),
			shard.WithMode(shCfg.Mode),
			shard.WithBlobStorOptions(
				blobstor.WithCompressObjects(*shCfg.Compress),
				blobstor.WithUncompressableContentTypes(shCfg.CompressionExcludeContentTypes),
				blobstor.WithStorages(ss),

				blobstor.WithLogger(c.log),
			),
			shard.WithMetaBaseOptions(
				meta.WithPath(shCfg.Metabase.Path),
				meta.WithPermissions(shCfg.Metabase.Perm),
				meta.WithMaxBatchSize(int(shCfg.Metabase.MaxBatchSize)),
				meta.WithMaxBatchDelay(shCfg.Metabase.MaxBatchDelay),
				meta.WithBoltDBOptions(&bbolt.Options{
					Timeout: time.Second,
				}),

				meta.WithLogger(c.log),
				meta.WithEpochState(c.cfgNetmap.state),
				meta.WithContainers(containerPresenceChecker{src: c.cnrSrc}),
				meta.WithInitContext(c.ctx),
			),
			shard.WithWriteCache(*shCfg.WriteCache.Enabled),
			shard.WithWriteCacheOptions(writeCacheOpts...),
			shard.WithRemoverBatchSize(int(shCfg.GC.RemoverBatchSize)),
			shard.WithGCRemoverSleepInterval(shCfg.GC.RemoverSleepInterval),
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
