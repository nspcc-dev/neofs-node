package config

import (
	"fmt"
	"strings"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/internal"
	"github.com/nspcc-dev/neofs-node/internal/configutil"
	"github.com/spf13/viper"
)

const separator = "."

// New creates a new Config instance.
//
// If file option is provided (WithConfigFile),
// configuration values are read from it.
// Otherwise, Config is filled with default values.
func New(opts ...Option) (*Config, error) {
	v := viper.New()

	v.SetEnvPrefix(internal.EnvPrefix)
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(separator, internal.EnvSeparator))

	o := defaultOpts()
	for i := range opts {
		opts[i](o)
	}

	if o.path != "" {
		v.SetConfigFile(o.path)

		err := v.ReadInConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to read config: %w", err)
		}
	}

	var cfg Config
	err := configutil.Unmarshal(v, &cfg, internal.EnvPrefix, internal.SizeHook(), internal.ModeHook())
	if err != nil {
		return nil, fmt.Errorf("failed config unmarshal: %w", err)
	}
	cfg.Normalize()
	cfg.opts = o

	return &cfg, nil
}

// Path returns the file system path to the configuration file
// being used by the Config instance. If no path was set, it
// returns an empty string.
func (c *Config) Path() string {
	return c.opts.path
}
