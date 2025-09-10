package fschainconfig_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	fschainconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/fschain"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestFSChainSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Empty(t, empty.FSChain.Endpoints)
		require.Equal(t, fschainconfig.DialTimeoutDefault, empty.FSChain.DialTimeout)
		require.Equal(t, fschainconfig.CacheTTLDefault, empty.FSChain.CacheTTL)
		require.Equal(t, fschainconfig.QuotaTTLDefault, empty.FSChain.QuotaCacheTTL)
		require.Equal(t, 5, empty.FSChain.ReconnectionsNumber)
		require.Equal(t, 5*time.Second, empty.FSChain.ReconnectionsDelay)
	})

	const path = "../../../../config/example/node"

	rpcs := []string{"wss://rpc1.morph.fs.neo.org:40341/ws", "wss://rpc2.morph.fs.neo.org:40341/ws"}

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, rpcs, c.FSChain.Endpoints)
		require.Equal(t, 30*time.Second, c.FSChain.DialTimeout)
		require.Equal(t, time.Minute, c.FSChain.QuotaCacheTTL)
		require.Equal(t, 15*time.Second, c.FSChain.CacheTTL)
		require.Equal(t, 6, c.FSChain.ReconnectionsNumber)
		require.Equal(t, 6*time.Second, c.FSChain.ReconnectionsDelay)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
