package config_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/stretchr/testify/require"
)

func TestConfigCommon(t *testing.T) {
	forEachFileType("test/config", func(c *config.Config) {
		val := c.Value("value")
		require.NotNil(t, val)

		val = c.Value("non-existent value")
		require.Nil(t, val)

		sub := c.Sub("section")
		require.NotNil(t, sub)

		const nonExistentSub = "non-existent sub-section"

		sub = c.Sub(nonExistentSub)
		require.Nil(t, sub)

		val = c.Sub(nonExistentSub).Value("value")
		require.Nil(t, val)
	})
}
