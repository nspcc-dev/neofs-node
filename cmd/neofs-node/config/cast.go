package config

import (
	"math/bits"
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

// StringSlice reads a configuration value
// from c by name and casts it to a []string.
//
// Panics if the value can not be casted.
func StringSlice(c *Config, name string) []string {
	x, err := cast.ToStringSliceE(c.Value(name))
	panicOnErr(err)

	return x
}

// StringSliceSafe reads a configuration value
// from c by name and casts it to a []string.
//
// Returns nil if the value can not be casted.
func StringSliceSafe(c *Config, name string) []string {
	return cast.ToStringSlice(c.Value(name))
}

// String reads a configuration value
// from c by name and casts it to a string.
//
// Panics if the value can not be casted.
func String(c *Config, name string) string {
	x, err := cast.ToStringE(c.Value(name))
	panicOnErr(err)

	return x
}

// StringSafe reads a configuration value
// from c by name and casts it to a string.
//
// Returns "" if the value can not be casted.
func StringSafe(c *Config, name string) string {
	return cast.ToString(c.Value(name))
}

// Duration reads a configuration value
// from c by name and casts it to time.Duration.
//
// Panics if the value can not be casted.
func Duration(c *Config, name string) time.Duration {
	x, err := cast.ToDurationE(c.Value(name))
	panicOnErr(err)

	return x
}

// DurationSafe reads a configuration value
// from c by name and casts it to time.Duration.
//
// Returns 0 if the value can not be casted.
func DurationSafe(c *Config, name string) time.Duration {
	return cast.ToDuration(c.Value(name))
}

// Bool reads a configuration value
// from c by name and casts it to bool.
//
// Panics if the value can not be casted.
func Bool(c *Config, name string) bool {
	x, err := cast.ToBoolE(c.Value(name))
	panicOnErr(err)

	return x
}

// BoolSafe reads a configuration value
// from c by name and casts it to bool.
//
// Returns false if the value can not be casted.
func BoolSafe(c *Config, name string) bool {
	return cast.ToBool(c.Value(name))
}

// Uint32 reads a configuration value
// from c by name and casts it to uint32.
//
// Panics if the value can not be casted.
func Uint32(c *Config, name string) uint32 {
	x, err := cast.ToUint32E(c.Value(name))
	panicOnErr(err)

	return x
}

// Uint32Safe reads a configuration value
// from c by name and casts it to uint32.
//
// Returns 0 if the value can not be casted.
func Uint32Safe(c *Config, name string) uint32 {
	return cast.ToUint32(c.Value(name))
}

// Uint reads a configuration value
// from c by name and casts it to uint64.
//
// Panics if the value can not be casted.
func Uint(c *Config, name string) uint64 {
	x, err := cast.ToUint64E(c.Value(name))
	panicOnErr(err)

	return x
}

// UintSafe reads a configuration value
// from c by name and casts it to uint64.
//
// Returns 0 if the value can not be casted.
func UintSafe(c *Config, name string) uint64 {
	return cast.ToUint64(c.Value(name))
}

// Int reads a configuration value
// from c by name and casts it to int64.
//
// Panics if the value can not be casted.
func Int(c *Config, name string) int64 {
	x, err := cast.ToInt64E(c.Value(name))
	panicOnErr(err)

	return x
}

// IntSafe reads a configuration value
// from c by name and casts it to int64.
//
// Returns 0 if the value can not be casted.
func IntSafe(c *Config, name string) int64 {
	return cast.ToInt64(c.Value(name))
}

// SizeInBytesSafe reads a configuration value
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

// safeMul returns size*multiplier, rounding down to the
// multiplier/1024 number of bytes.
// Returns 0 if overflow is detected.
func safeMul(size float64, multiplier uint64) uint64 {
	n := uint64(size)
	f := uint64((size - float64(n)) * 1024)
	if f != 0 && multiplier != 1 {
		s := n<<10 + f
		if s < n {
			return 0
		}

		n = s
		multiplier >>= 10
	}

	hi, lo := bits.Mul64(n, multiplier)
	if hi != 0 {
		return 0
	}
	return lo
}

// parseSizeInBytes converts strings like 1GB or 12 mb into an unsigned integer number of bytes.
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

	size := cast.ToFloat64(sizeStr)
	return safeMul(size, multiplier)
}
