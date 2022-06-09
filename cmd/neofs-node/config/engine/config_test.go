package engineconfig_test

import (
	"io/fs"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	piloramaconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/pilorama"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/stretchr/testify/require"
)

func TestEngineSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Panics(t, func() {
			engineconfig.IterateShards(empty, true, nil)
		})

		handlerCalled := false

		require.NotPanics(t, func() {
			engineconfig.IterateShards(empty, false, func(_ *shardconfig.Config) {
				handlerCalled = true
			})
		})

		require.False(t, handlerCalled)

		require.EqualValues(t, 0, engineconfig.ShardErrorThreshold(empty))
		require.EqualValues(t, engineconfig.ShardPoolSizeDefault, engineconfig.ShardPoolSize(empty))
		require.EqualValues(t, shard.ModeReadWrite, shardconfig.From(empty).Mode())
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		num := 0

		require.EqualValues(t, 100, engineconfig.ShardErrorThreshold(c))
		require.EqualValues(t, 15, engineconfig.ShardPoolSize(c))

		engineconfig.IterateShards(c, true, func(sc *shardconfig.Config) {
			defer func() {
				num++
			}()

			wc := sc.WriteCache()
			meta := sc.Metabase()
			blob := sc.BlobStor()
			blz := blob.Blobovnicza()
			pl := sc.Pilorama()
			gc := sc.GC()

			switch num {
			case 0:
				require.Equal(t, "tmp/0/blob/pilorama.db", pl.Path())
				require.Equal(t, fs.FileMode(piloramaconfig.PermDefault), pl.Perm())
				require.False(t, pl.NoSync())
				require.Equal(t, pl.MaxBatchDelay(), 10*time.Millisecond)
				require.Equal(t, pl.MaxBatchSize(), 200)

				require.Equal(t, false, wc.Enabled())

				require.Equal(t, "tmp/0/cache", wc.Path())
				require.EqualValues(t, 2147483648, wc.MemSize())
				require.EqualValues(t, 16384, wc.SmallObjectSize())
				require.EqualValues(t, 134217728, wc.MaxObjectSize())
				require.EqualValues(t, 30, wc.WorkersNumber())
				require.EqualValues(t, 3221225472, wc.SizeLimit())

				require.Equal(t, "tmp/0/meta", meta.Path())
				require.Equal(t, fs.FileMode(0644), meta.BoltDB().Perm())
				require.Equal(t, 100, meta.BoltDB().MaxBatchSize())
				require.Equal(t, 10*time.Millisecond, meta.BoltDB().MaxBatchDelay())

				require.Equal(t, "tmp/0/blob", blob.Path())
				require.EqualValues(t, 0644, blob.Perm())
				require.Equal(t, true, blob.Compress())
				require.Equal(t, []string{"audio/*", "video/*"}, blob.UncompressableContentTypes())
				require.EqualValues(t, 5, blob.ShallowDepth())
				require.EqualValues(t, 102400, blob.SmallSizeLimit())

				require.EqualValues(t, 4194304, blz.Size())
				require.EqualValues(t, 1, blz.ShallowDepth())
				require.EqualValues(t, 4, blz.ShallowWidth())
				require.EqualValues(t, 50, blz.OpenedCacheSize())

				require.EqualValues(t, 150, gc.RemoverBatchSize())
				require.Equal(t, 2*time.Minute, gc.RemoverSleepInterval())

				require.Equal(t, false, sc.RefillMetabase())
				require.Equal(t, shard.ModeReadOnly, sc.Mode())
			case 1:
				require.Equal(t, "tmp/1/blob/pilorama.db", pl.Path())
				require.Equal(t, fs.FileMode(0644), pl.Perm())
				require.True(t, pl.NoSync())
				require.Equal(t, 5*time.Millisecond, pl.MaxBatchDelay())
				require.Equal(t, 100, pl.MaxBatchSize())

				require.Equal(t, true, wc.Enabled())

				require.Equal(t, "tmp/1/cache", wc.Path())
				require.EqualValues(t, 2147483648, wc.MemSize())
				require.EqualValues(t, 16384, wc.SmallObjectSize())
				require.EqualValues(t, 134217728, wc.MaxObjectSize())
				require.EqualValues(t, 30, wc.WorkersNumber())
				require.EqualValues(t, 4294967296, wc.SizeLimit())

				require.Equal(t, "tmp/1/meta", meta.Path())
				require.Equal(t, fs.FileMode(0644), meta.BoltDB().Perm())
				require.Equal(t, 200, meta.BoltDB().MaxBatchSize())
				require.Equal(t, 20*time.Millisecond, meta.BoltDB().MaxBatchDelay())

				require.Equal(t, "tmp/1/blob", blob.Path())
				require.EqualValues(t, 0644, blob.Perm())
				require.Equal(t, false, blob.Compress())
				require.Equal(t, []string(nil), blob.UncompressableContentTypes())
				require.EqualValues(t, 5, blob.ShallowDepth())
				require.EqualValues(t, 102400, blob.SmallSizeLimit())

				require.EqualValues(t, 4194304, blz.Size())
				require.EqualValues(t, 1, blz.ShallowDepth())
				require.EqualValues(t, 4, blz.ShallowWidth())
				require.EqualValues(t, 50, blz.OpenedCacheSize())

				require.EqualValues(t, 200, gc.RemoverBatchSize())
				require.Equal(t, 5*time.Minute, gc.RemoverSleepInterval())

				require.Equal(t, true, sc.RefillMetabase())
				require.Equal(t, shard.ModeReadWrite, sc.Mode())
			}
		})

		require.Equal(t, 2, num)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
