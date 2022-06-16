package metabaseconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	boltdbconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/boltdb"
)

// Config is a wrapper over the config section
// which provides access to Metabase configurations.
type Config config.Config

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// Path returns the value of "path" config parameter.
//
// Panics if the value is not a non-empty string.
func (x *Config) Path() string {
	p := config.String(
		(*config.Config)(x),
		"path",
	)

	if p == "" {
		panic("metabase path not set")
	}

	return p
}

// BoltDB returns config instance for querying bolt db specific parameters.
func (x *Config) BoltDB() *boltdbconfig.Config {
	return (*boltdbconfig.Config)(x)
}
