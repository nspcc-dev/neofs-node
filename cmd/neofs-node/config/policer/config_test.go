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

		require.Equal(t, policerconfig.HeadTimeoutDefault, empty.Policer.HeadTimeout)
		require.Equal(t, policerconfig.ReplicationCooldownDefault, empty.Policer.ReplicationCooldown)
		require.Equal(t, uint32(policerconfig.ObjectBatchSizeDefault), empty.Policer.ObjectBatchSize)
		require.Equal(t, uint32(policerconfig.MaxWorkersDefault), empty.Policer.MaxWorkers)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, 15*time.Second, c.Policer.HeadTimeout)
		require.Equal(t, 101*time.Millisecond, c.Policer.ReplicationCooldown)
		require.Equal(t, uint32(11), c.Policer.ObjectBatchSize)
		require.Equal(t, uint32(21), c.Policer.MaxWorkers)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
