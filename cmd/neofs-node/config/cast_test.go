package config_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/stretchr/testify/require"
)

func TestStringSlice(t *testing.T) {
	forEachFileType("test/config", func(c *config.Config) {
		cStringSlice := c.Sub("string_slice")

		val := config.StringSlice(cStringSlice, "empty")
		require.Empty(t, val)

		val = config.StringSlice(cStringSlice, "filled")
		require.Equal(t, []string{
			"string1",
			"string2",
		}, val)

		require.Panics(t, func() {
			config.StringSlice(cStringSlice, "incorrect")
		})

		val = config.StringSliceSafe(cStringSlice, "incorrect")
		require.Nil(t, val)
	})
}
