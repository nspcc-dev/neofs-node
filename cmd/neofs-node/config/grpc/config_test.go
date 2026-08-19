package grpcconfig_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestGRPCSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig(t)

		require.Zero(t, empty.GRPC)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		num := 0

		for i := range c.GRPC {
			sc := c.GRPC[i]
			tls := sc.TLS

			switch num {
			case 0:
				require.Equal(t, "s01.neofs.devenv:8080", sc.Endpoint)
				require.Equal(t, 1, sc.ConnLimit)

				require.NotNil(t, tls)
				require.Equal(t, "/path/to/cert", tls.Certificate)
			case 1:
				require.Equal(t, "s02.neofs.devenv:8080", sc.Endpoint)
				require.Equal(t, 0, sc.ConnLimit)
				require.Equal(t, grpcconfig.TLS{}, tls)
			case 2:
				require.Equal(t, "s03.neofs.devenv:8080", sc.Endpoint)
				require.Equal(t, 0, sc.ConnLimit)
				require.Equal(t, grpcconfig.TLS{}, tls)
			case 3:
				require.Equal(t, "s04.neofs.devenv:8080", sc.Endpoint)
				require.Equal(t, 0, sc.ConnLimit)
				require.True(t, tls.Enabled)
				require.Empty(t, tls.Certificate)
			}
			num++
		}

		require.Equal(t, 4, num)
	}

	configtest.ForEachFileType(t, path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(t, path, fileConfigTest)
	})
}
