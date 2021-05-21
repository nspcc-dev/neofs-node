package config

import (
	"strings"
)

// Sub returns subsection of the Config by name.
//
// Returns nil if subsection if missing.
func (x *Config) Sub(name string) *Config {
	return &Config{
		v:    x.v,
		path: append(x.path, name),
	}
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
	return x.v.Get(strings.Join(append(x.path, name), separator))
}
