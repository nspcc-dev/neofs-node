package policerconfig_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	policerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/policer"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestPolicerSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Equal(t, policerconfig.HeadTimeoutDefault, policerconfig.HeadTimeout(empty))
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, 15*time.Second, policerconfig.HeadTimeout(c))
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
