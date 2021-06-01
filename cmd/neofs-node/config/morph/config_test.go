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

		require.Panics(t, func() { morphconfig.RPCEndpoint(empty) })
		require.Panics(t, func() { morphconfig.NotificationEndpoint(empty) })
		require.Equal(t, morphconfig.DialTimeoutDefault, morphconfig.DialTimeout(empty))
	})

	const path = "../../../../config/example/node"

	var (
		rpcs = []string{
			"https://rpc1.morph.fs.neo.org:40341",
			"https://rpc2.morph.fs.neo.org:40341",
		}

		wss = []string{
			"wss://rpc1.morph.fs.neo.org:40341/ws",
			"wss://rpc2.morph.fs.neo.org:40341/ws",
		}
	)

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, rpcs, morphconfig.RPCEndpoint(c))
		require.Equal(t, wss, morphconfig.NotificationEndpoint(c))
		require.Equal(t, 30*time.Second, morphconfig.DialTimeout(c))
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
