package controlconfig_test

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestControlSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Empty(t, empty.Control.AuthorizedKeys)
		require.Empty(t, empty.Control.GRPC.Endpoint)
	})

	const path = "../../../../config/example/node"

	pubs := make(keys.PublicKeys, 2)
	pubs[0], _ = keys.NewPublicKeyFromString("035839e45d472a3b7769a2a1bd7d54c4ccd4943c3b40f547870e83a8fcbfb3ce11")
	pubs[1], _ = keys.NewPublicKeyFromString("028f42cfcb74499d7b15b35d9bff260a1c8d27de4f446a627406a382d8961486d6")

	var fileConfigTest = func(c *config.Config) {
		require.Equal(t, pubs, c.Control.AuthorizedKeys)
		require.Equal(t, "localhost:8090", c.Control.GRPC.Endpoint)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
