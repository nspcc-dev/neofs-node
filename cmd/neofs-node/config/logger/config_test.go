package loggerconfig_test

import (
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/internal"
	loggerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/logger"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestLoggerSection_Level(t *testing.T) {
	configtest.ForEachFileType("../../../../config/example/node", func(c *config.Config) {
		v := loggerconfig.Level(c)
		require.Equal(t, "debug", v)
	})

	v := loggerconfig.Level(configtest.EmptyConfig())
	require.Equal(t, loggerconfig.LevelDefault, v)

	t.Run("ENV", func(t *testing.T) {
		// TODO: read from file
		err := os.Setenv(
			internal.Env("logger", "level"),
			"debug",
		)
		require.NoError(t, err)

		v := loggerconfig.Level(configtest.EmptyConfig())
		require.Equal(t, "debug", v)
	})
}
