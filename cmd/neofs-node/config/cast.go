package config

import (
	"github.com/spf13/cast"
)

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

// StringSlice reads configuration value
// from c by name and casts it to []string.
//
// Panics if value can not be casted.
func StringSlice(c *Config, name string) []string {
	x, err := cast.ToStringSliceE(c.Value(name))
	panicOnErr(err)

	return x
}

// StringSliceSafe reads configuration value
// from c by name and casts it to []string.
//
// Returns nil if value can not be casted.
func StringSliceSafe(c *Config, name string) []string {
	return cast.ToStringSlice(c.Value(name))
}
