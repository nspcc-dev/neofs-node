package config

import (
	"bytes"
	"path"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/innerring"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestGenerateConfigExample(t *testing.T) {
	const (
		n      = 10
		appDir = "/home/example/.neofs"
	)

	configText, err := generateConfigExample(appDir, n)
	require.NoError(t, err)

	v := viper.New()
	v.SetConfigType("yml")

	require.NoError(t, v.ReadConfig(bytes.NewBufferString(configText)))

	require.Equal(t, "https://neo.rpc.node:30333", v.GetString("rpc-endpoint"))
	require.Equal(t, path.Join(appDir, "alphabet-wallets"), v.GetString("alphabet-wallets"))
	require.Equal(t, 67108864, v.GetInt("network.max_object_size"))
	require.Equal(t, 240, v.GetInt("network.epoch_duration"))

	var i innerring.GlagoliticLetter
	for i = 0; i < innerring.GlagoliticLetter(n); i++ {
		key := "credentials." + i.String()
		require.Equal(t, "password", v.GetString(key))
	}

	key := "credentials." + i.String()
	require.Equal(t, "", v.GetString(key))
}
