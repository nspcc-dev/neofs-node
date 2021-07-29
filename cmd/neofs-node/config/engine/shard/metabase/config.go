package metabaseconfig

import (
	"io/fs"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// Config is a wrapper over the config section
// which provides access to Metabase configurations.
type Config config.Config

// config defaults
const (
	// PermDefault is a default permission bits for metabase file.
	PermDefault = 0660
)

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// Path returns value of "path" config parameter.
//
// Panics if value is not a non-empty string.
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

// Perm returns value of "perm" config parameter as a fs.FileMode.
//
// Returns PermDefault if value is not a positive number.
func (x *Config) Perm() fs.FileMode {
	p := config.UintSafe(
		(*config.Config)(x),
		"perm",
	)

	if p == 0 {
		p = PermDefault
	}

	return fs.FileMode(p)
}
