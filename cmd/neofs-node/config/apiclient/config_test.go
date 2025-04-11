package apiclientconfig_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	apiclientconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/apiclient"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestApiclientSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Equal(t, apiclientconfig.StreamTimeoutDefault, empty.APIClient.StreamTimeout)
		require.Equal(t, apiclientconfig.MinConnTimeDefault, empty.APIClient.MinConnectionTime)
		require.Equal(t, apiclientconfig.PingIntervalDefault, empty.APIClient.PingInterval)
		require.Equal(t, apiclientconfig.PingTimeoutDefault, empty.APIClient.PingTimeout)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, 20*time.Second, c.APIClient.StreamTimeout)
		require.Equal(t, 30*time.Second, c.APIClient.MinConnectionTime)
		require.Equal(t, 20*time.Second, c.APIClient.PingInterval)
		require.Equal(t, 10*time.Second, c.APIClient.PingTimeout)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
