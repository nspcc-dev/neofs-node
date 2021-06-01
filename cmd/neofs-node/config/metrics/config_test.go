package metricsconfig_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	metricsconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/metrics"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestMetricsSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		to := metricsconfig.ShutdownTimeout(configtest.EmptyConfig())
		addr := metricsconfig.Address(configtest.EmptyConfig())

		require.Equal(t, metricsconfig.ShutdownTimeoutDefault, to)
		require.Equal(t, metricsconfig.AddressDefault, addr)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		to := metricsconfig.ShutdownTimeout(c)
		addr := metricsconfig.Address(c)

		require.Equal(t, 15*time.Second, to)
		require.Equal(t, "127.0.0.1:9090", addr)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
