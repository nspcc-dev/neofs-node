package nodeconfig

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/stretchr/testify/require"
)

func TestNodeSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.PanicsWithError(
			t,
			errKeyNotSet.Error(),
			func() {
				Key(empty)
			},
		)

		require.PanicsWithError(
			t,
			errAddressNotSet.Error(),
			func() {
				BootstrapAddress(empty)
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
		addr := BootstrapAddress(c)
		attributes := Attributes(c)
		relay := Relay(c)

		expectedAddr, err := network.AddressFromString("s01.neofs.devenv:8080")
		require.NoError(t, err)

		require.Equal(t, "path/hex/WIF", key)
		require.Equal(t, true, addr.Equal(expectedAddr))
		require.Equal(t, true, relay)

		require.Len(t, attributes, 2)
		require.Equal(t, "Price:11", attributes[0])
		require.Equal(t, "UN-LOCODE:RU MSK", attributes[1])
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
