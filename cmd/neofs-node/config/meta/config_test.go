package metaconfig_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestLoggerSection_Level(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		emptyConfig := configtest.EmptyConfig(t)
		require.Equal(t, "", emptyConfig.Meta.Path)
		require.Zero(t, emptyConfig.Meta.SeedPort)
		require.Nil(t, emptyConfig.Meta.P2PAddresses)
		require.Nil(t, emptyConfig.Meta.RPCAddresses)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, "path/to/meta", c.Meta.Path)
		require.Equal(t, uint16(20334), c.Meta.SeedPort)
		require.Equal(t, []string{"node1:20334", "node2:20334"}, c.Meta.P2PAddresses)
		require.Equal(t, []string{"localhost:30334"}, c.Meta.RPCAddresses)
	}

	configtest.ForEachFileType(t, path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(t, path, fileConfigTest)
	})
}
