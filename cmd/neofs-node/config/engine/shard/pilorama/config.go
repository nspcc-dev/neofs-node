package piloramaconfig

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
// Returns empty string if missing, for compatibility with older configurations.
func (x *Config) Path() string {
	return config.String((*config.Config)(x), "path")
}

// BoltDB returns config instance for querying bolt db specific parameters.
func (x *Config) BoltDB() *boltdbconfig.Config {
	return (*boltdbconfig.Config)(x)
}
