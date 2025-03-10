package nodeconfig

import (
	"slices"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestNodeSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Panics(
			t,
			func() {
				BootstrapAddresses(empty)
			},
		)

		attribute := Attributes(empty)
		relay := Relay(empty)
		persisessionsPath := PersistentSessions(empty).Path()
		persistatePath := PersistentState(empty).Path()

		require.Empty(t, attribute)
		require.Equal(t, false, relay)
		require.Equal(t, "", persisessionsPath)
		require.Equal(t, PersistentStatePathDefault, persistatePath)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		addrs := BootstrapAddresses(c)
		attributes := Attributes(c)
		relay := Relay(c)
		wKey := Wallet(c)
		persisessionsPath := PersistentSessions(c).Path()
		persistatePath := PersistentState(c).Path()

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
			config.StringSafe(c.Sub("node").Sub("wallet"), "address"),
			address.Uint160ToString(wKey.GetScriptHash()))

		require.Equal(t, "/sessions", persisessionsPath)
		require.Equal(t, "/state", persistatePath)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
