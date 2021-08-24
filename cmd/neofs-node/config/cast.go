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

// Bool reads configuration value
// from c by name and casts it to bool.
//
// Panics if value can not be casted.
func Bool(c *Config, name string) bool {
	x, err := cast.ToBoolE(c.Value(name))
	panicOnErr(err)

	return x
}

// BoolSafe reads configuration value
// from c by name and casts it to bool.
//
// Returns false if value can not be casted.
func BoolSafe(c *Config, name string) bool {
	return cast.ToBool(c.Value(name))
}

// Uint32 reads configuration value
// from c by name and casts it to uint32.
//
// Panics if value can not be casted.
func Uint32(c *Config, name string) uint32 {
	x, err := cast.ToUint32E(c.Value(name))
	panicOnErr(err)

	return x
}

// Uint32Safe reads configuration value
// from c by name and casts it to uint32.
//
// Returns 0 if value can not be casted.
func Uint32Safe(c *Config, name string) uint32 {
	return cast.ToUint32(c.Value(name))
}

// Uint reads configuration value
// from c by name and casts it to uint64.
//
// Panics if value can not be casted.
func Uint(c *Config, name string) uint64 {
	x, err := cast.ToUint64E(c.Value(name))
	panicOnErr(err)

	return x
}

// UintSafe reads configuration value
// from c by name and casts it to uint64.
//
// Returns 0 if value can not be casted.
func UintSafe(c *Config, name string) uint64 {
	return cast.ToUint64(c.Value(name))
}

// Int reads configuration value
// from c by name and casts it to int64.
//
// Panics if value can not be casted.
func Int(c *Config, name string) int64 {
	x, err := cast.ToInt64E(c.Value(name))
	panicOnErr(err)

	return x
}

// IntSafe reads configuration value
// from c by name and casts it to int64.
//
// Returns 0 if value can not be casted.
func IntSafe(c *Config, name string) int64 {
	return cast.ToInt64(c.Value(name))
}
