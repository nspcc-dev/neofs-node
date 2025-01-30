package engineconfig_test

import (
	"io/fs"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	fstreeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/fstree"
	piloramaconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/pilorama"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/stretchr/testify/require"
)

func TestEngineSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.ErrorIs(t,
			engineconfig.IterateShards(empty, true, nil),
			engineconfig.ErrNoShardConfigured)

		handlerCalled := false

		require.NoError(t,
			engineconfig.IterateShards(empty, false, func(_ *shardconfig.Config) error {
				handlerCalled = true
				return nil
			}))

		require.False(t, handlerCalled)

		require.EqualValues(t, 0, engineconfig.ShardErrorThreshold(empty))
		require.EqualValues(t, engineconfig.ShardPoolSizeDefault, engineconfig.ShardPoolSize(empty))
		require.EqualValues(t, mode.ReadWrite, shardconfig.From(empty).Mode())
		require.Zero(t, engineconfig.ObjectPutRetryDeadline(empty))
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		num := 0

		require.EqualValues(t, 100, engineconfig.ShardErrorThreshold(c))
		require.EqualValues(t, 15, engineconfig.ShardPoolSize(c))
		require.EqualValues(t, 5*time.Second, engineconfig.ObjectPutRetryDeadline(c))
		require.EqualValues(t, true, engineconfig.IgnoreUninitedShards(c))

		err := engineconfig.IterateShards(c, true, func(sc *shardconfig.Config) error {
			defer func() {
				num++
			}()

			wc := sc.WriteCache()
			meta := sc.Metabase()
			blob := sc.BlobStor()
			ss := blob.Storages()
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
				require.Equal(t, true, wc.NoSync())

				require.Equal(t, "tmp/0/cache", wc.Path())
				require.EqualValues(t, 134217728, wc.MaxObjectSize())
				require.EqualValues(t, 3221225472, wc.SizeLimit())

				require.Equal(t, "tmp/0/meta", meta.Path())
				require.Equal(t, fs.FileMode(0644), meta.BoltDB().Perm())
				require.Equal(t, 100, meta.BoltDB().MaxBatchSize())
				require.Equal(t, 10*time.Millisecond, meta.BoltDB().MaxBatchDelay())

				require.Equal(t, true, sc.Compress())
				require.Equal(t, []string{"audio/*", "video/*"}, sc.UncompressableContentTypes())
				require.EqualValues(t, 102400, sc.SmallSizeLimit())

				require.Equal(t, 2, len(ss))
				require.Equal(t, "tmp/0/blob/peapod.db", ss[0].Path())
				require.EqualValues(t, 0644, ss[0].Perm())
				require.EqualValues(t, peapod.Type, ss[0].Type())
				require.EqualValues(t, 10*time.Millisecond, ss[0].FlushInterval())

				require.Equal(t, "tmp/0/blob", ss[1].Path())
				require.EqualValues(t, 0644, ss[1].Perm())

				fst := fstreeconfig.From((*config.Config)(ss[1]))
				require.EqualValues(t, 5, fst.Depth())
				require.Equal(t, false, fst.NoSync())

				require.EqualValues(t, 150, gc.RemoverBatchSize())
				require.Equal(t, 2*time.Minute, gc.RemoverSleepInterval())

				require.Equal(t, false, sc.ResyncMetabase())
				require.Equal(t, mode.ReadOnly, sc.Mode())
			case 1:
				require.Equal(t, "tmp/1/blob/pilorama.db", pl.Path())
				require.Equal(t, fs.FileMode(0644), pl.Perm())
				require.True(t, pl.NoSync())
				require.Equal(t, 5*time.Millisecond, pl.MaxBatchDelay())
				require.Equal(t, 100, pl.MaxBatchSize())

				require.Equal(t, true, wc.Enabled())
				require.Equal(t, false, wc.NoSync())

				require.Equal(t, "tmp/1/cache", wc.Path())
				require.EqualValues(t, 134217728, wc.MaxObjectSize())
				require.EqualValues(t, 4294967296, wc.SizeLimit())

				require.Equal(t, "tmp/1/meta", meta.Path())
				require.Equal(t, fs.FileMode(0644), meta.BoltDB().Perm())
				require.Equal(t, 200, meta.BoltDB().MaxBatchSize())
				require.Equal(t, 20*time.Millisecond, meta.BoltDB().MaxBatchDelay())

				require.Equal(t, false, sc.Compress())
				require.Equal(t, []string(nil), sc.UncompressableContentTypes())
				require.EqualValues(t, 102400, sc.SmallSizeLimit())

				require.Equal(t, 2, len(ss))
				require.Equal(t, "tmp/1/blob/peapod.db", ss[0].Path())
				require.EqualValues(t, 0644, ss[0].Perm())
				require.EqualValues(t, peapod.Type, ss[0].Type())
				require.EqualValues(t, 30*time.Millisecond, ss[0].FlushInterval())

				require.Equal(t, "tmp/1/blob", ss[1].Path())
				require.EqualValues(t, 0644, ss[1].Perm())

				fst := fstreeconfig.From((*config.Config)(ss[1]))
				require.EqualValues(t, 5, fst.Depth())
				require.Equal(t, true, fst.NoSync())

				require.EqualValues(t, 200, gc.RemoverBatchSize())
				require.Equal(t, 5*time.Minute, gc.RemoverSleepInterval())

				require.Equal(t, true, sc.ResyncMetabase())
				require.Equal(t, mode.ReadWrite, sc.Mode())
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, num)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
