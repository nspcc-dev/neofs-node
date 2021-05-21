package config_test

import (
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/internal"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestConfigCommon(t *testing.T) {
	configtest.ForEachFileType("test/config", func(c *config.Config) {
		val := c.Value("value")
		require.NotNil(t, val)

		val = c.Value("non-existent value")
		require.Nil(t, val)

		sub := c.Sub("section")
		require.NotNil(t, sub)

		const nonExistentSub = "non-existent sub-section"

		val = c.Sub(nonExistentSub).Value("value")
		require.Nil(t, val)
	})
}

func TestConfigEnv(t *testing.T) {
	const (
		name    = "name"
		section = "section"
		value   = "some value"
	)

	err := os.Setenv(internal.Env(section, name), value)
	require.NoError(t, err)

	c := configtest.EmptyConfig()

	require.Equal(t, value, c.Sub(section).Value(name))
}
