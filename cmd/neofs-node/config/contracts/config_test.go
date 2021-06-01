package contractsconfig_test

import (
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	contractsconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/contracts"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestContractsSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Panics(t, func() { contractsconfig.Balance(empty) })
		require.Panics(t, func() { contractsconfig.Container(empty) })
		require.Panics(t, func() { contractsconfig.Netmap(empty) })
		require.Panics(t, func() { contractsconfig.Reputation(empty) })
	})

	const path = "../../../../config/example/node"

	expBalance, err := util.Uint160DecodeStringLE("5263abba1abedbf79bb57f3e40b50b4425d2d6cd")
	require.NoError(t, err)

	expConatiner, err := util.Uint160DecodeStringLE("5d084790d7aa36cea7b53fe897380dab11d2cd3c")
	require.NoError(t, err)

	expNetmap, err := util.Uint160DecodeStringLE("0cce9e948dca43a6b592efe59ddb4ecb89bdd9ca")
	require.NoError(t, err)

	expReputation, err := util.Uint160DecodeStringLE("441995f631c1da2b133462b71859494a5cd45e90")
	require.NoError(t, err)

	var fileConfigTest = func(c *config.Config) {
		balance := contractsconfig.Balance(c)
		container := contractsconfig.Container(c)
		netmap := contractsconfig.Netmap(c)
		reputation := contractsconfig.Reputation(c)

		require.Equal(t, expBalance, balance)
		require.Equal(t, expConatiner, container)
		require.Equal(t, expNetmap, netmap)
		require.Equal(t, expReputation, reputation)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
