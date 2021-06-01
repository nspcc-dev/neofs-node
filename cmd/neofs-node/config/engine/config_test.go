package engineconfig_test

import (
	"os"
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
				require.EqualValues(t, 111, wc.MemSize())
				require.EqualValues(t, 222, wc.MaxDBSize())
				require.EqualValues(t, 333, wc.SmallObjectSize())
				require.EqualValues(t, 444, wc.MaxObjectSize())
				require.EqualValues(t, 555, wc.WorkersNumber())

				require.Equal(t, "tmp/0/meta", meta.Path())
				require.Equal(t, os.FileMode(0700), meta.Perm())

				require.Equal(t, "tmp/0/blob", blob.Path())
				require.EqualValues(t, 0666, blob.Perm())
				require.EqualValues(t, 5, blob.ShallowDepth())
				require.Equal(t, true, blob.Compress())
				require.EqualValues(t, 77, blob.SmallSizeLimit())

				require.EqualValues(t, 1024, blz.Size())
				require.EqualValues(t, 10, blz.ShallowDepth())
				require.EqualValues(t, 20, blz.ShallowWidth())
				require.EqualValues(t, 88, blz.OpenedCacheSize())

				require.EqualValues(t, 123, gc.RemoverBatchSize())
				require.Equal(t, 3*time.Hour, gc.RemoverSleepInterval())
			case 1:
				require.Equal(t, true, sc.UseWriteCache())

				require.Equal(t, "tmp/1/cache", wc.Path())
				require.EqualValues(t, 112, wc.MemSize())
				require.EqualValues(t, 223, wc.MaxDBSize())
				require.EqualValues(t, 334, wc.SmallObjectSize())
				require.EqualValues(t, 445, wc.MaxObjectSize())
				require.EqualValues(t, 556, wc.WorkersNumber())

				require.Equal(t, "tmp/1/meta", meta.Path())
				require.Equal(t, os.FileMode(0701), meta.Perm())

				require.Equal(t, "tmp/1/blob", blob.Path())
				require.EqualValues(t, 0667, blob.Perm())
				require.EqualValues(t, 6, blob.ShallowDepth())
				require.Equal(t, false, blob.Compress())
				require.EqualValues(t, 78, blob.SmallSizeLimit())

				require.EqualValues(t, 1025, blz.Size())
				require.EqualValues(t, 11, blz.ShallowDepth())
				require.EqualValues(t, 21, blz.ShallowWidth())
				require.EqualValues(t, 89, blz.OpenedCacheSize())

				require.EqualValues(t, 124, gc.RemoverBatchSize())
				require.Equal(t, 3*time.Hour+time.Second, gc.RemoverSleepInterval())
			}
		})

		require.Equal(t, 2, num)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
