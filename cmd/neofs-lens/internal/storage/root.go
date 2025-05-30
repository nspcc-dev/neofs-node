package storage

import (
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/util"
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
	appCfg, err := config.New(config.WithConfigFile(vConfig))
	if err != nil {
		return nil, fmt.Errorf("failed to load config file: %w", err)
	}

	ls := engine.New()

	var shardsWithMeta []shardOptsWithID
	for _, shCfg := range appCfg.Storage.ShardList {
		var (
			wcMaxBatchSize      uint64
			wcMaxBatchCount     int
			wcMaxBatchThreshold uint64
		)
		var s common.Storage
		sRead := shCfg.Blobstor
		switch sRead.Type {
		case fstree.Type:
			wcMaxBatchSize = uint64(sRead.CombinedSizeLimit)
			wcMaxBatchCount = sRead.CombinedCountLimit
			wcMaxBatchThreshold = uint64(sRead.CombinedSizeThreshold)
			s = fstree.New(
				fstree.WithPath(sRead.Path),
				fstree.WithPerm(sRead.Perm),
				fstree.WithDepth(sRead.Depth),
				fstree.WithNoSync(*sRead.NoSync),
				fstree.WithCombinedCountLimit(sRead.CombinedCountLimit),
				fstree.WithCombinedSizeLimit(int(sRead.CombinedSizeLimit)),
				fstree.WithCombinedSizeThreshold(int(sRead.CombinedSizeThreshold)),
				fstree.WithCombinedWriteInterval(sRead.FlushInterval))
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
				writecache.WithMaxFlushBatchSize(wcMaxBatchSize),
				writecache.WithMaxFlushBatchCount(wcMaxBatchCount),
				writecache.WithMaxFlushBatchThreshold(wcMaxBatchThreshold),
			)
		}

		var sh shardOptsWithID
		sh.configID = shCfg.ID()
		sh.shOpts = []shard.Option{
			shard.WithResyncMetabase(*shCfg.ResyncMetabase),
			shard.WithMode(shCfg.Mode),
			shard.WithCompressObjects(*shCfg.Compress),
			shard.WithUncompressableContentTypes(shCfg.CompressionExcludeContentTypes),
			shard.WithBlobstor(s),
			shard.WithMetaBaseOptions(
				meta.WithPath(shCfg.Metabase.Path),
				meta.WithPermissions(shCfg.Metabase.Perm),
				meta.WithMaxBatchSize(int(shCfg.Metabase.MaxBatchSize)),
				meta.WithMaxBatchDelay(shCfg.Metabase.MaxBatchDelay),
				meta.WithBoltDBOptions(&bbolt.Options{
					Timeout: time.Second,
				}),

				meta.WithEpochState(epochState{}),
			),
			shard.WithWriteCache(*shCfg.WriteCache.Enabled),
			shard.WithWriteCacheOptions(writeCacheOpts...),
			shard.WithRemoverBatchSize(int(shCfg.GC.RemoverBatchSize)),
			shard.WithGCRemoverSleepInterval(shCfg.GC.RemoverSleepInterval),
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
