package config

import (
	"github.com/spf13/viper"
)

// Sub returns sub-section of the Config by name.
func (x *Config) Sub(name string) *Config {
	return (*Config)(
		(*viper.Viper)(x).Sub(name),
	)
}

// Value returns configuration value by name.
//
// Result can be casted to a particular type
// via corresponding function (e.g. StringSlice).
// Note: casting via Go `.()` operator is not
// recommended.
//
// Returns nil if config is nil.
func (x *Config) Value(name string) interface{} {
	if x != nil {
		return (*viper.Viper)(x).Get(name)
	}

	return nil
}
