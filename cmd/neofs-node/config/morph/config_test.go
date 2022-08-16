package morphconfig_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	morphconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/morph"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/stretchr/testify/require"
)

func TestMorphSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Panics(t, func() { morphconfig.RPCEndpoint(empty) })
		require.Equal(t, morphconfig.DialTimeoutDefault, morphconfig.DialTimeout(empty))
		require.Equal(t, morphconfig.CacheTTLDefault, morphconfig.CacheTTL(empty))
	})

	const path = "../../../../config/example/node"

	var (
		rpcs = []client.Endpoint{
			{"wss://rpc1.morph.fs.neo.org:40341/ws", 1},
			{"wss://rpc2.morph.fs.neo.org:40341/ws", 2},
		}
	)

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, rpcs, morphconfig.RPCEndpoint(c))
		require.Equal(t, 30*time.Second, morphconfig.DialTimeout(c))
		require.Equal(t, 15*time.Second, morphconfig.CacheTTL(c))
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
