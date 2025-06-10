package loggerconfig_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	loggerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/logger"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestLoggerSection_Level(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		emptyConfig := configtest.EmptyConfig()
		require.Equal(t, loggerconfig.LevelDefault, emptyConfig.Logger.Level)
		require.Equal(t, loggerconfig.EncodingDefault, emptyConfig.Logger.Encoding)
		require.False(t, emptyConfig.Logger.Timestamp)
		require.False(t, emptyConfig.Logger.Sampling.Enabled)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, "debug", c.Logger.Level)
		require.Equal(t, "json", c.Logger.Encoding)
		require.True(t, c.Logger.Sampling.Enabled)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
