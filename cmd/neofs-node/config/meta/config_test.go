package metaconfig_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	metaconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/meta"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestLoggerSection_Level(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		emptyConfig := configtest.EmptyConfig()
		require.Equal(t, "", metaconfig.Path(emptyConfig))
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, "path/to/meta", metaconfig.Path(c))
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
