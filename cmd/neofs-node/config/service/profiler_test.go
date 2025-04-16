package serviceconfig_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	serviceconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/service"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestProfilerSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		emptyConfig := configtest.EmptyConfig()
		require.Equal(t, serviceconfig.ShutdownTimeoutDefault, emptyConfig.Pprof.ShutdownTimeout)
		require.Equal(t, serviceconfig.ProfilerAddressDefault, emptyConfig.Pprof.Address)
		require.False(t, emptyConfig.Pprof.Enabled)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, 15*time.Second, c.Pprof.ShutdownTimeout)
		require.Equal(t, "localhost:6060", c.Pprof.Address)
		require.True(t, c.Pprof.Enabled)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
