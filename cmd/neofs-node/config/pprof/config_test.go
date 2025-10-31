package pprofconfig_test

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	pprofconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/pprof"
	serviceconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/service"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestPprof(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		emptyConfig := configtest.EmptyConfig()
		require.Equal(t, pprofconfig.Pprof{
			Service: serviceconfig.Service{
				Enabled:         false,
				Address:         "localhost:6060",
				ShutdownTimeout: 30 * time.Second,
			},
			EnableBlock: false,
			EnableMutex: false,
		}, emptyConfig.Pprof)
	})

	const path = "../../../../config/example/node"

	fileConfigTest := func(c *config.Config) {
		require.Equal(t, pprofconfig.Pprof{
			Service: serviceconfig.Service{
				Enabled:         true,
				Address:         "localhost:6161",
				ShutdownTimeout: 15 * time.Second,
			},
			EnableBlock: true,
			EnableMutex: true,
		}, c.Pprof)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
