package objectconfig_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	objectconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/object"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestObjectSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Equal(t, objectconfig.PutPoolSizeDefault, empty.Object.Put.PoolSizeRemote)
		require.EqualValues(t, objectconfig.DefaultTombstoneLifetime, empty.Object.Delete.TombstoneLifetime)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, 100, c.Object.Put.PoolSizeRemote)
		require.EqualValues(t, 10, c.Object.Delete.TombstoneLifetime)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
