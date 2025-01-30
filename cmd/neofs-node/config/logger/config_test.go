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
		require.Equal(t, loggerconfig.LevelDefault, loggerconfig.Level(emptyConfig))
		require.Equal(t, loggerconfig.EncodingDefault, loggerconfig.Encoding(emptyConfig))
		require.False(t, loggerconfig.Timestamp(emptyConfig))
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, "debug", loggerconfig.Level(c))
		require.Equal(t, "json", loggerconfig.Encoding(c))
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
