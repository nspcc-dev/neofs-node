package treeconfig_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	treeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/tree"
	"github.com/stretchr/testify/require"
)

func TestTreeSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		treeSec := treeconfig.Tree(empty)

		require.False(t, treeSec.Enabled())
		require.Equal(t, 0, treeSec.CacheSize())
		require.Equal(t, 0, treeSec.ReplicationChannelCapacity())
		require.Equal(t, 0, treeSec.ReplicationWorkerCount())
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		treeSec := treeconfig.Tree(c)

		require.True(t, treeSec.Enabled())
		require.Equal(t, 15, treeSec.CacheSize())
		require.Equal(t, 32, treeSec.ReplicationChannelCapacity())
		require.Equal(t, 32, treeSec.ReplicationWorkerCount())
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
