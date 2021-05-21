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
	checkLevel := func(c *loggerconfig.LoggerSection, expected string) {
		lvl := c.Level()
		require.Equal(t, expected, lvl)
	}

	configtest.ForEachFileType("../../../../config/example/node", func(c *config.Config) {
		cfg := loggerconfig.Init(c)

		checkLevel(cfg, "debug")
	})

	empty := loggerconfig.Init(configtest.EmptyConfig())

	checkLevel(empty, loggerconfig.LevelDefault)

	t.Run("ENV", func(t *testing.T) {
		// TODO: read from file
		err := os.Setenv(
			internal.Env("logger", "level"),
			"debug",
		)
		require.NoError(t, err)

		empty = loggerconfig.Init(configtest.EmptyConfig())

		checkLevel(empty, "debug")
	})
}
