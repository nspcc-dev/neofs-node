package mainchainconfig_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	mainchainconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/mainchain"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestMainchainSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Empty(t, mainchainconfig.RPCEndpoint(empty))
		require.Equal(t, mainchainconfig.DialTimeoutDefault, mainchainconfig.DialTimeout(empty))
	})

	const path = "../../../../config/example/node"

	var rpcs = []string{
		"https://rpc1.n3.nspcc.ru:30341",
		"https://rpc2.n3.nspcc.ru:30341",
	}

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, rpcs, mainchainconfig.RPCEndpoint(c))
		require.Equal(t, 30*time.Second, mainchainconfig.DialTimeout(c))
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
