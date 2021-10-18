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

func TestConfig_SubValue(t *testing.T) {
	configtest.ForEachFileType("test/config", func(c *config.Config) {
		c = c.
			Sub("section").
			Sub("sub").
			Sub("sub")

		// get subsection 1
		sub := c.Sub("sub1")

		// get subsection 2
		c.Sub("sub2")

		// sub should not be corrupted
		require.Equal(t, "val1", sub.Value("key"))
	})
}

func TestConfig_SetDefault(t *testing.T) {
	configtest.ForEachFileType("test/config", func(c *config.Config) {
		c = c.Sub("with_default")
		s := c.Sub("custom")
		s.SetDefault(c.Sub("default"))

		require.Equal(t, int64(42), config.Int(s, "missing"))
		require.Equal(t, "b", config.String(s, "overridden"))
		require.Equal(t, false, config.Bool(s, "overridden_with_default"))

		// Default can be set only once.
		s = s.Sub("sub")
		require.Equal(t, int64(123), config.Int(s, "missing"))
		require.Equal(t, "y", config.String(s, "overridden"))
	})
}
