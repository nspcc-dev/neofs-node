package config

import (
	"strings"
)

// Sub returns subsection of the Config by name.
//
// Returns nil if subsection if missing.
func (x *Config) Sub(name string) *Config {
	// copy path in order to prevent consequent violations
	ln := len(x.path)

	path := make([]string, ln, ln+1)

	copy(path, x.path)

	var defaultPath []string
	if x.defaultPath != nil {
		ln := len(x.defaultPath)
		defaultPath = make([]string, ln, ln+1)
		copy(defaultPath, x.defaultPath)
	}

	return &Config{
		v:           x.v,
		path:        append(path, name),
		defaultPath: append(defaultPath, name),
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
	value := x.v.Get(strings.Join(append(x.path, name), separator))
	if value != nil || x.defaultPath == nil {
		return value
	}
	return x.v.Get(strings.Join(append(x.defaultPath, name), separator))
}

// SetDefault sets fallback config for missing values.
//
// It supports only one level of nesting and is intended to be used
// to provide default values.
func (x *Config) SetDefault(from *Config) {
	x.defaultPath = make([]string, len(from.path))
	copy(x.defaultPath, from.path)
}
