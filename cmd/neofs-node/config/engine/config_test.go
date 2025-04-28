package engineconfig_test

import (
	"io/fs"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/internal"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/stretchr/testify/require"
)

func TestEngineSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.ErrorIs(t,
			engineconfig.IterateShards(&empty.Storage, true, nil),
			engineconfig.ErrNoShardConfigured)

		handlerCalled := false

		require.NoError(t,
			engineconfig.IterateShards(&empty.Storage, false, func(*shardconfig.ShardDetails) error {
				handlerCalled = true
				return nil
			}))

		require.False(t, handlerCalled)

		require.EqualValues(t, 0, empty.Storage.ShardROErrorThreshold)
		require.EqualValues(t, engineconfig.ShardPoolSizeDefault, empty.Storage.ShardPoolSize)
		require.EqualValues(t, mode.ReadWrite, empty.Storage.Default.Mode)
		require.Zero(t, empty.Storage.PutRetryTimeout)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		num := 0

		require.EqualValues(t, 100, c.Storage.ShardROErrorThreshold)
		require.EqualValues(t, 15, c.Storage.ShardPoolSize)
		require.EqualValues(t, 5*time.Second, c.Storage.PutRetryTimeout)
		require.EqualValues(t, true, c.Storage.IgnoreUninitedShards)

		err := engineconfig.IterateShards(&c.Storage, true, func(sc *shardconfig.ShardDetails) error {
			defer func() {
				num++
			}()

			wc := sc.WriteCache
			meta := sc.Metabase
			ss := sc.Blobstor
			gc := sc.GC

			switch num {
			case 0:
				require.False(t, *wc.Enabled)
				require.True(t, *wc.NoSync)

				require.Equal(t, "tmp/0/cache", wc.Path)
				require.EqualValues(t, 134217728, wc.MaxObjectSize)
				require.EqualValues(t, 3221225472, wc.Capacity)

				require.Equal(t, "tmp/0/meta", meta.Path)
				require.Equal(t, fs.FileMode(0644), meta.Perm)
				require.Equal(t, internal.Size(100), meta.MaxBatchSize)
				require.Equal(t, 10*time.Millisecond, meta.MaxBatchDelay)

				require.True(t, *sc.Compress)
				require.Equal(t, []string{"audio/*", "video/*"}, sc.CompressionExcludeContentTypes)

				require.Equal(t, "tmp/0/blob", ss.Path)
				require.EqualValues(t, 0644, ss.Perm)
				require.EqualValues(t, 5, ss.Depth)
				require.False(t, *ss.NoSync)

				require.EqualValues(t, 150, gc.RemoverBatchSize)
				require.Equal(t, 2*time.Minute, gc.RemoverSleepInterval)

				require.False(t, *sc.ResyncMetabase)
				require.Equal(t, mode.ReadOnly, sc.Mode)
			case 1:
				require.True(t, *wc.Enabled)
				require.False(t, *wc.NoSync)

				require.Equal(t, "tmp/1/cache", wc.Path)
				require.EqualValues(t, 134217728, wc.MaxObjectSize)
				require.EqualValues(t, 4294967296, wc.Capacity)

				require.Equal(t, "tmp/1/meta", meta.Path)
				require.Equal(t, fs.FileMode(0644), meta.Perm)
				require.EqualValues(t, 200, meta.MaxBatchSize)
				require.Equal(t, 20*time.Millisecond, meta.MaxBatchDelay)

				require.False(t, *sc.Compress)
				require.Equal(t, []string(nil), sc.CompressionExcludeContentTypes)

				require.Equal(t, "tmp/1/blob", ss.Path)
				require.EqualValues(t, 0644, ss.Perm)

				require.EqualValues(t, 5, ss.Depth)
				require.True(t, *ss.NoSync)

				require.EqualValues(t, 200, gc.RemoverBatchSize)
				require.Equal(t, 5*time.Minute, gc.RemoverSleepInterval)

				require.True(t, *sc.ResyncMetabase)
				require.Equal(t, mode.ReadWrite, sc.Mode)
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
