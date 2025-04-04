package storage

import (
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	fstreeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/storage"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/panjf2000/ants/v2"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt"
)

var (
	vAddress     string
	vOut         string
	vConfig      string
	vPayloadOnly bool
)

var Root = &cobra.Command{
	Use:   "storage",
	Short: "Operations with blobstor",
	Args:  cobra.NoArgs,
}

func init() {
	Root.AddCommand(
		storageGetObjCMD,
		storageListObjsCMD,
		storageStatusObjCMD,
		storageSanityCMD,
	)
}

type shardOptsWithID struct {
	configID string
	shOpts   []shard.Option
}

type epochState struct {
}

func (e epochState) CurrentEpoch() uint64 {
	return 0
}

func openEngine() (*engine.StorageEngine, error) {
	appCfg := config.New(config.Prm{}, config.WithConfigFile(vConfig))

	ls := engine.New()

	var shards []storage.ShardCfg
	err := engineconfig.IterateShards(appCfg, false, func(sc *shardconfig.Config) error {
		var sh storage.ShardCfg

		sh.ResyncMetabase = sc.ResyncMetabase()
		sh.Mode = sc.Mode()
		sh.Compress = sc.Compress()
		sh.UncompressableContentType = sc.UncompressableContentTypes()
		sh.SmallSizeObjectLimit = sc.SmallSizeLimit()

		// write-cache

		writeCacheCfg := sc.WriteCache()
		if writeCacheCfg.Enabled() {
			wc := &sh.WritecacheCfg

			wc.Enabled = true
			wc.Path = writeCacheCfg.Path()
			wc.MaxObjSize = writeCacheCfg.MaxObjectSize()
			wc.SizeLimit = writeCacheCfg.SizeLimit()
			wc.NoSync = writeCacheCfg.NoSync()
		}

		// blobstor with substorages

		blobStorCfg := sc.BlobStor()
		storagesCfg := blobStorCfg.Storages()
		metabaseCfg := sc.Metabase()
		gcCfg := sc.GC()

		ss := make([]storage.SubStorageCfg, 0, len(storagesCfg))
		for i := range storagesCfg {
			var sCfg storage.SubStorageCfg

			sCfg.Typ = storagesCfg[i].Type()
			sCfg.Path = storagesCfg[i].Path()
			sCfg.Perm = storagesCfg[i].Perm()
			sCfg.FlushInterval = storagesCfg[i].FlushInterval()

			switch storagesCfg[i].Type() {
			case fstree.Type:
				sub := fstreeconfig.From((*config.Config)(storagesCfg[i]))
				sCfg.Depth = sub.Depth()
				sCfg.NoSync = sub.NoSync()
				sCfg.CombinedCountLimit = sub.CombinedCountLimit()
				sCfg.CombinedSizeLimit = sub.CombinedSizeLimit()
				sCfg.CombinedSizeThreshold = sub.CombinedSizeThreshold()
			case peapod.Type:
				// Nothing peapod-specific, but it should work.
			default:
				return fmt.Errorf("can't initiate storage. invalid storage type: %s", storagesCfg[i].Type())
			}

			ss = append(ss, sCfg)
		}

		sh.SubStorages = ss

		// meta

		m := &sh.MetaCfg

		m.Path = metabaseCfg.Path()
		m.Perm = metabaseCfg.BoltDB().Perm()
		m.MaxBatchDelay = metabaseCfg.BoltDB().MaxBatchDelay()
		m.MaxBatchSize = metabaseCfg.BoltDB().MaxBatchSize()

		// GC

		sh.GcCfg.RemoverBatchSize = gcCfg.RemoverBatchSize()
		sh.GcCfg.RemoverSleepInterval = gcCfg.RemoverSleepInterval()

		shards = append(shards, sh)

		return nil
	})
	if err != nil {
		return nil, err
	}

	var shardsWithMeta []shardOptsWithID
	for _, shCfg := range shards {
		var writeCacheOpts []writecache.Option
		if wcRead := shCfg.WritecacheCfg; wcRead.Enabled {
			writeCacheOpts = append(writeCacheOpts,
				writecache.WithPath(wcRead.Path),
				writecache.WithMaxObjectSize(wcRead.MaxObjSize),
				writecache.WithMaxCacheSize(wcRead.SizeLimit),
				writecache.WithNoSync(wcRead.NoSync),
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
			shard.WithResyncMetabase(shCfg.ResyncMetabase),
			shard.WithMode(shCfg.Mode),
			shard.WithBlobStorOptions(
				blobstor.WithCompressObjects(shCfg.Compress),
				blobstor.WithUncompressableContentTypes(shCfg.UncompressableContentType),
				blobstor.WithStorages(ss),
			),
			shard.WithMetaBaseOptions(
				meta.WithPath(shCfg.MetaCfg.Path),
				meta.WithPermissions(shCfg.MetaCfg.Perm),
				meta.WithMaxBatchSize(shCfg.MetaCfg.MaxBatchSize),
				meta.WithMaxBatchDelay(shCfg.MetaCfg.MaxBatchDelay),
				meta.WithBoltDBOptions(&bbolt.Options{
					Timeout: time.Second,
				}),

				meta.WithEpochState(epochState{}),
			),
			shard.WithWriteCache(shCfg.WritecacheCfg.Enabled),
			shard.WithWriteCacheOptions(writeCacheOpts...),
			shard.WithRemoverBatchSize(shCfg.GcCfg.RemoverBatchSize),
			shard.WithGCRemoverSleepInterval(shCfg.GcCfg.RemoverSleepInterval),
			shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
				pool, poolErr := ants.NewPool(sz)
				if poolErr != nil {
					err = poolErr
				}
				return pool
			}),
		}
		if err != nil {
			return nil, err
		}

		shardsWithMeta = append(shardsWithMeta, sh)
	}

	for _, optsWithMeta := range shardsWithMeta {
		_, err := ls.AddShard(append(optsWithMeta.shOpts, shard.WithMode(mode.ReadOnly))...)
		if err != nil {
			return nil, err
		}
	}

	if err := ls.Open(); err != nil {
		return nil, err
	}
	if err := ls.Init(); err != nil {
		return nil, err
	}

	return ls, nil
}
