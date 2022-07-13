package profilerconfig_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	profilerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/profiler"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestProfilerSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		to := profilerconfig.ShutdownTimeout(configtest.EmptyConfig())
		addr := profilerconfig.Address(configtest.EmptyConfig())

		require.Equal(t, profilerconfig.ShutdownTimeoutDefault, to)
		require.Equal(t, profilerconfig.AddressDefault, addr)
		require.False(t, profilerconfig.Enabled(configtest.EmptyConfig()))
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		to := profilerconfig.ShutdownTimeout(c)
		addr := profilerconfig.Address(c)

		require.Equal(t, 15*time.Second, to)
		require.Equal(t, "localhost:6060", addr)
		require.True(t, profilerconfig.Enabled(c))
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
