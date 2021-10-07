package config

import (
	"strings"
	"time"
	"unicode"

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

// SizeInBytesSafe reads configuration value
// from c by name and casts it to size in bytes (uint64).
//
// The suffix can be single-letter (b, k, m, g, t) or with
// an additional b at the end. Spaces between the number and the suffix
// are allowed. All multipliers are power of 2 (i.e. k is for kibi-byte).
//
// Returns 0 if a value can't be casted.
func SizeInBytesSafe(c *Config, name string) uint64 {
	s := StringSafe(c, name)
	return parseSizeInBytes(s)
}

// The following code is taken from https://github.com/spf13/viper/blob/master/util.go
// with minor corrections (allow to use both `k` and `kb` forms.
// Seems like viper allows to convert sizes but corresponding parser in `cast` package
// is missing.
func safeMul(a, b uint64) uint64 {
	c := a * b
	if a > 1 && b > 1 && c/b != a {
		return 0
	}
	return c
}

// parseSizeInBytes converts strings like 1GB or 12 mb into an unsigned integer number of bytes
func parseSizeInBytes(sizeStr string) uint64 {
	sizeStr = strings.TrimSpace(sizeStr)
	lastChar := len(sizeStr) - 1
	multiplier := uint64(1)

	if lastChar > 0 {
		if sizeStr[lastChar] == 'b' || sizeStr[lastChar] == 'B' {
			lastChar--
		}
		if lastChar > 0 {
			switch unicode.ToLower(rune(sizeStr[lastChar])) {
			case 'k':
				multiplier = 1 << 10
				sizeStr = strings.TrimSpace(sizeStr[:lastChar])
			case 'm':
				multiplier = 1 << 20
				sizeStr = strings.TrimSpace(sizeStr[:lastChar])
			case 'g':
				multiplier = 1 << 30
				sizeStr = strings.TrimSpace(sizeStr[:lastChar])
			case 't':
				multiplier = 1 << 40
				sizeStr = strings.TrimSpace(sizeStr[:lastChar])
			default:
				multiplier = 1
				sizeStr = strings.TrimSpace(sizeStr[:lastChar+1])
			}
		}
	}

	size := cast.ToUint64(sizeStr)
	return safeMul(size, multiplier)
}
