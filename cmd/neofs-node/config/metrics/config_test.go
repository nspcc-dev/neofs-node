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
		emptyConfig := configtest.EmptyConfig()

		require.Equal(t, metricsconfig.ShutdownTimeoutDefault, emptyConfig.Prometheus.ShutdownTimeout)
		require.Equal(t, metricsconfig.AddressDefault, emptyConfig.Prometheus.Address)
		require.False(t, emptyConfig.Prometheus.Enabled)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, 15*time.Second, c.Prometheus.ShutdownTimeout)
		require.Equal(t, "localhost:9090", c.Prometheus.Address)
		require.True(t, c.Prometheus.Enabled)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
