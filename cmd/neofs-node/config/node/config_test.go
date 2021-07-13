package nodeconfig

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/stretchr/testify/require"
)

func TestNodeSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Panics(
			t,
			func() {
				Key(empty)
			},
		)

		require.Panics(
			t,
			func() {
				BootstrapAddresses(empty)
			},
		)

		attribute := Attributes(empty)
		relay := Relay(empty)

		require.Empty(t, attribute)
		require.Equal(t, false, relay)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		key := Key(c)
		addrs := BootstrapAddresses(c)
		attributes := Attributes(c)
		relay := Relay(c)
		wKey := Wallet(c)

		expectedAddr := []struct {
			str  string
			host string
			tls  bool
		}{
			{
				str:  "/dns4/localhost/tcp/8083/tls",
				host: "localhost:8083",
				tls:  true,
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

		require.Equal(t, "NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM", key.Address())

		require.EqualValues(t, len(expectedAddr), addrs.Len())

		ind := 0

		addrs.IterateAddresses(func(addr network.Address) bool {
			require.Equal(t, expectedAddr[ind].str, addr.String())
			require.Equal(t, expectedAddr[ind].host, addr.HostAddr())
			require.Equal(t, expectedAddr[ind].tls, addr.TLSEnabled())

			ind++

			return false
		})

		require.Equal(t, true, relay)

		require.Len(t, attributes, 2)
		require.Equal(t, "Price:11", attributes[0])
		require.Equal(t, "UN-LOCODE:RU MSK", attributes[1])

		require.NotNil(t, wKey)
		require.Equal(t,
			config.StringSafe(c.Sub("node").Sub("wallet"), "address"),
			address.Uint160ToString(wKey.GetScriptHash()))
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
