package config

import (
	"time"

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

// String reads configuration value
// from c by name and casts it to string.
//
// Panics if value can not be casted.
func String(c *Config, name string) string {
	x, err := cast.ToStringE(c.Value(name))
	panicOnErr(err)

	return x
}

// StringSafe reads configuration value
// from c by name and casts it to string.
//
// Returns "" if value can not be casted.
func StringSafe(c *Config, name string) string {
	return cast.ToString(c.Value(name))
}

// Duration reads configuration value
// from c by name and casts it to time.Duration.
//
// Panics if value can not be casted.
func Duration(c *Config, name string) time.Duration {
	x, err := cast.ToDurationE(c.Value(name))
	panicOnErr(err)

	return x
}

// DurationSafe reads configuration value
// from c by name and casts it to time.Duration.
//
// Returns 0 if value can not be casted.
func DurationSafe(c *Config, name string) time.Duration {
	return cast.ToDuration(c.Value(name))
}
