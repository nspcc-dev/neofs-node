package nodeconfig_test

import (
	"slices"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestNodeSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Panics(
			t,
			func() {
				empty.Node.BootstrapAddresses()
			},
		)

		require.Empty(t, empty.Node.Attributes)
		require.Equal(t, false, empty.Node.Relay)
		require.Equal(t, "", empty.Node.PersistentSessions.Path)
		require.Equal(t, nodeconfig.PersistentStatePathDefault, empty.Node.PersistentState.Path)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		addrs := c.Node.BootstrapAddresses()
		attributes := c.Node.Attributes
		relay := c.Node.Relay
		wKey := c.Node.PrivateKey()
		persisessionsPath := c.Node.PersistentSessions.Path
		persistatePath := c.Node.PersistentState.Path

		expectedAddr := []struct {
			str  string
			host string
		}{
			{
				str:  "/dns4/localhost/tcp/8083/tls",
				host: "grpcs://localhost:8083",
			},
			{
				str:  "/dns4/s01.neofs.devenv/tcp/8080",
				host: "s01.neofs.devenv:8080",
			},
			{
				str:  "/dns4/s02.neofs.devenv/tcp/8081",
				host: "s02.neofs.devenv:8081",
			},
			{
				str:  "/ip4/127.0.0.1/tcp/8082",
				host: "127.0.0.1:8082",
			},
		}

		require.EqualValues(t, len(expectedAddr), addrs.Len())

		ind := 0

		for addr := range slices.Values(addrs) {
			require.Equal(t, expectedAddr[ind].str, addr.String())
			require.Equal(t, expectedAddr[ind].host, addr.URIAddr())

			ind++
		}

		require.Equal(t, true, relay)

		require.Len(t, attributes, 3)
		require.Equal(t, "Price:11", attributes[0])
		require.Equal(t, "UN-LOCODE:RU MSK", attributes[1])
		require.Equal(t, "VerifiedNodesDomain:nodes.some-org.neofs", attributes[2])

		require.NotNil(t, wKey)
		require.Equal(t,
			c.Node.Wallet.Address,
			address.Uint160ToString(wKey.GetScriptHash()))

		require.Equal(t, "/sessions", persisessionsPath)
		require.Equal(t, "/state", persistatePath)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
