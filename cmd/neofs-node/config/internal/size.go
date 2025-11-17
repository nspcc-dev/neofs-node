package internal

import (
	"math"
	"math/bits"
	"reflect"
	"strings"
	"unicode"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/cast"
)

// Size is an unsigned integer value that represents a size in bytes.
type Size uint64

// Check checks if the size is valid and sets it to the default if it is not.
func (s *Size) Check(def, defConst Size) {
	if *s <= 0 || *s > math.MaxInt {
		if def <= 0 || def > math.MaxInt {
			*s = defConst
		} else {
			*s = def
		}
	}
}

// SizeHook returns a mapstructure decode hook func that converts a string to a Size.
func SizeHook() mapstructure.DecodeHookFuncType {
	return func(_ reflect.Type, t reflect.Type, data any) (any, error) {
		if t != reflect.TypeFor[Size]() {
			return data, nil
		}

		str := cast.ToString(data)
		return parseSizeInBytes(str), nil
	}
}

// The following code is taken from https://github.com/spf13/viper/blob/master/util.go
// with minor corrections (allow to use both `k` and `kb` forms).
// Seems like viper allows to convert sizes but corresponding parser in `cast` package
// is missing.

// safeMul returns size*multiplier.
// Returns 0 if overflow is detected.
func safeMul(size uint64, multiplier uint64) uint64 {
	hi, lo := bits.Mul64(size, multiplier)
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
		if lastChar >= 0 {
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
