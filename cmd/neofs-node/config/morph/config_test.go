package morphconfig_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	morphconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/morph"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestMorphSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Panics(t, func() { morphconfig.Endpoints(empty) })
		require.Equal(t, morphconfig.DialTimeoutDefault, morphconfig.DialTimeout(empty))
		require.Equal(t, morphconfig.CacheTTLDefault, morphconfig.CacheTTL(empty))
		require.Equal(t, 5, morphconfig.ReconnectionRetriesNumber(empty))
		require.Equal(t, 5*time.Second, morphconfig.ReconnectionRetriesDelay(empty))
	})

	const path = "../../../../config/example/node"

	rpcs := []string{"wss://rpc1.morph.fs.neo.org:40341/ws", "wss://rpc2.morph.fs.neo.org:40341/ws"}

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, rpcs, morphconfig.Endpoints(c))
		require.Equal(t, 30*time.Second, morphconfig.DialTimeout(c))
		require.Equal(t, 15*time.Second, morphconfig.CacheTTL(c))
		require.Equal(t, 6, morphconfig.ReconnectionRetriesNumber(c))
		require.Equal(t, 6*time.Second, morphconfig.ReconnectionRetriesDelay(c))
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
