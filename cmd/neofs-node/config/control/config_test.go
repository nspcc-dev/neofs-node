package controlconfig_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	controlconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/control"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestControlSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Empty(t, controlconfig.AuthorizedKeysString(empty))
		require.Equal(t, controlconfig.GRPCEndpointDefault, controlconfig.GRPC(empty).Endpoint())
	})

	const path = "../../../../config/example/node"

	var keys = []string{
		"035839e45d472a3b7769a2a1bd7d54c4ccd4943c3b40f547870e83a8fcbfb3ce11",
		"028f42cfcb74499d7b15b35d9bff260a1c8d27de4f446a627406a382d8961486d6",
	}

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, keys, controlconfig.AuthorizedKeysString(c))
		require.Equal(t, "127.0.0.1:8090", controlconfig.GRPC(c).Endpoint())
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
