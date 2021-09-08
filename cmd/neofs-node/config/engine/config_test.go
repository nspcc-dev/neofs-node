package engineconfig_test

import (
	"io/fs"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestEngineSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		require.Panics(t, func() {
			engineconfig.IterateShards(configtest.EmptyConfig(), nil)
		})
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		num := 0

		engineconfig.IterateShards(c, func(sc *shardconfig.Config) {
			defer func() {
				num++
			}()

			wc := sc.WriteCache()
			meta := sc.Metabase()
			blob := sc.BlobStor()
			blz := blob.Blobovnicza()
			gc := sc.GC()

			switch num {
			case 0:
				require.Equal(t, false, sc.UseWriteCache())

				require.Equal(t, "tmp/0/cache", wc.Path())
				require.EqualValues(t, 2147483648, wc.MemSize())
				require.EqualValues(t, 16384, wc.SmallObjectSize())
				require.EqualValues(t, 134217728, wc.MaxObjectSize())
				require.EqualValues(t, 30, wc.WorkersNumber())
				require.EqualValues(t, 3221225472, wc.SizeLimit())

				require.Equal(t, "tmp/0/meta", meta.Path())
				require.Equal(t, fs.FileMode(0644), meta.Perm())

				require.Equal(t, "tmp/0/blob", blob.Path())
				require.EqualValues(t, 0644, blob.Perm())
				require.Equal(t, true, blob.Compress())
				require.EqualValues(t, 5, blob.ShallowDepth())
				require.EqualValues(t, 102400, blob.SmallSizeLimit())

				require.EqualValues(t, 4194304, blz.Size())
				require.EqualValues(t, 1, blz.ShallowDepth())
				require.EqualValues(t, 4, blz.ShallowWidth())
				require.EqualValues(t, 50, blz.OpenedCacheSize())

				require.EqualValues(t, 150, gc.RemoverBatchSize())
				require.Equal(t, 2*time.Minute, gc.RemoverSleepInterval())
			case 1:
				require.Equal(t, true, sc.UseWriteCache())

				require.Equal(t, "tmp/1/cache", wc.Path())
				require.EqualValues(t, 2147483648, wc.MemSize())
				require.EqualValues(t, 16384, wc.SmallObjectSize())
				require.EqualValues(t, 134217728, wc.MaxObjectSize())
				require.EqualValues(t, 30, wc.WorkersNumber())
				require.EqualValues(t, 4294967296, wc.SizeLimit())

				require.Equal(t, "tmp/1/meta", meta.Path())
				require.Equal(t, fs.FileMode(0644), meta.Perm())

				require.Equal(t, "tmp/1/blob", blob.Path())
				require.EqualValues(t, 0644, blob.Perm())
				require.Equal(t, false, blob.Compress())
				require.EqualValues(t, 5, blob.ShallowDepth())
				require.EqualValues(t, 102400, blob.SmallSizeLimit())

				require.EqualValues(t, 4194304, blz.Size())
				require.EqualValues(t, 1, blz.ShallowDepth())
				require.EqualValues(t, 4, blz.ShallowWidth())
				require.EqualValues(t, 50, blz.OpenedCacheSize())

				require.EqualValues(t, 200, gc.RemoverBatchSize())
				require.Equal(t, 5*time.Minute, gc.RemoverSleepInterval())
			}
		})

		require.Equal(t, 2, num)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
