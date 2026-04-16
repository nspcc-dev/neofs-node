package signed256

import (
	"fmt"
	"strconv"

	"github.com/holiman/uint256"
)

const EncodedLen = 33

var maxAbs = func() uint256.Int {
	var z uint256.Int
	for i := range z {
		z[i] = ^uint64(0)
	}
	return z
}()

// Int is a signed integer in the inclusive range [-maxUint256; maxUint256].
// It stores sign separately from the absolute uint256 magnitude.
type Int struct {
	mag uint256.Int
	neg bool
}

// ParseDecimal parses a base-10 signed integer in the inclusive range
// [-maxUint256; maxUint256].
func ParseDecimal(s string) (Int, error) {
	var z Int
	return z, z.SetFromDecimal(s)
}

// ParseNormalizedDecimal parses already normalized base-10 digits with
// separately provided sign. digits must be non-empty and contain only decimal
// digits without leading sign.
func ParseNormalizedDecimal(neg bool, digits string) (Int, error) {
	var z Int
	if digits == "" {
		return z, fmt.Errorf("missing digits")
	}
	for i := range digits {
		if digits[i] < '0' || digits[i] > '9' {
			return z, fmt.Errorf("invalid decimal digit %q", digits[i])
		}
	}
	if len(digits) <= 20 {
		v, err := strconv.ParseUint(digits, 10, 64)
		if err == nil {
			z.mag.SetUint64(v)
			z.neg = neg && v != 0
			return z, nil
		}
	}
	if err := z.mag.SetFromDecimal(digits); err != nil {
		return z, err
	}
	z.neg = neg && !z.mag.IsZero()
	return z, nil
}

// MustParseDecimal parses s and panics on error.
func MustParseDecimal(s string) Int {
	z, err := ParseDecimal(s)
	if err != nil {
		panic(err)
	}
	return z
}

// NewInt returns a signed value initialized from v.
func NewInt(v int64) Int {
	var z Int
	if v >= 0 {
		z.mag.SetUint64(uint64(v))
		return z
	}

	z.neg = true
	if v == -1<<63 {
		z.mag.SetUint64(1 << 63)
		return z
	}
	z.mag.SetUint64(uint64(-v))
	return z
}

// NewUint64 returns an unsigned value initialized from v.
func NewUint64(v uint64) Int {
	var z Int
	z.mag.SetUint64(v)
	return z
}

// Max returns the maximal supported value.
func Max() Int {
	return Int{mag: maxAbs}
}

// Min returns the minimal supported value.
func Min() Int {
	z := Max()
	z.neg = true
	return z
}

// SetFromDecimal parses a base-10 signed integer in the inclusive range
// [-maxUint256; maxUint256].
func (z *Int) SetFromDecimal(s string) error {
	z.neg = false
	z.mag.Clear()

	if s == "" {
		return fmt.Errorf("empty decimal string")
	}

	switch s[0] {
	case '+':
		s = s[1:]
	case '-':
		z.neg = true
		s = s[1:]
	}
	if s == "" {
		return fmt.Errorf("missing digits")
	}

	if err := z.mag.SetFromDecimal(s); err != nil {
		return err
	}
	if z.mag.IsZero() {
		z.neg = false
	}
	return nil
}

// SetUint64 sets z to the unsigned value v.
func (z *Int) SetUint64(v uint64) *Int {
	z.neg = false
	z.mag.SetUint64(v)
	return z
}

// Add sets z to x+y and reports an error if the result is out of range.
func (z *Int) Add(x, y *Int) error {
	switch {
	case x.neg == y.neg:
		mag, overflow := new(uint256.Int).AddOverflow(&x.mag, &y.mag)
		if overflow {
			z.neg = false
			z.mag.Clear()
			return fmt.Errorf("value out of range")
		}
		z.mag = *mag
		z.neg = x.neg && !z.mag.IsZero()
		return nil
	case x.mag.Cmp(&y.mag) >= 0:
		z.mag.Sub(&x.mag, &y.mag)
		z.neg = x.neg && !z.mag.IsZero()
		return nil
	default:
		z.mag.Sub(&y.mag, &x.mag)
		z.neg = y.neg && !z.mag.IsZero()
		return nil
	}
}

// Cmp compares z and x and returns:
//   - -1 if z < x
//   - 0 if z == x
//   - +1 if z > x
func (z *Int) Cmp(x *Int) int {
	if z.neg != x.neg {
		if z.neg {
			return -1
		}
		return 1
	}

	cmp := z.mag.Cmp(&x.mag)
	if z.neg {
		return -cmp
	}
	return cmp
}

// String returns the base-10 representation of z.
func (z Int) String() string {
	if z.mag.IsZero() {
		return "0"
	}
	if z.neg {
		return "-" + z.mag.Dec()
	}
	return z.mag.Dec()
}

// FillBytes writes z in signed uint256 byte format into dst.
// dst must be at least EncodedLen bytes long.
func (z *Int) FillBytes(dst []byte) {
	if len(dst) < EncodedLen {
		panic(fmt.Errorf("insufficient buffer len %d", len(dst)))
	}

	if z.neg {
		dst[0] = 0
	} else {
		dst[0] = 1
	}
	raw := z.mag.Bytes32()
	copy(dst[1:EncodedLen], raw[:])
	if z.neg {
		for i := range dst[1:EncodedLen] {
			dst[1+i] = ^dst[1+i]
		}
	}
}

// EncodeBytes stores z in a signed uint256 byte format:
// 1 sign byte followed by 32 bytes of magnitude. Negative values are stored
// as bitwise-inverted magnitude bytes.
func (z *Int) EncodeBytes() [EncodedLen]byte {
	var out [EncodedLen]byte
	z.FillBytes(out[:])
	return out
}

// DecodeBytes parses signed uint256 byte format.
func DecodeBytes(b []byte) (Int, error) {
	if len(b) != EncodedLen {
		return Int{}, fmt.Errorf("invalid len %d", len(b))
	}

	var z Int
	switch b[0] {
	default:
		return Int{}, fmt.Errorf("invalid sign byte %d", b[0])
	case 0:
		z.neg = true
	case 1:
	}

	var raw [32]byte
	copy(raw[:], b[1:])
	if z.neg {
		for i := range raw {
			raw[i] = ^raw[i]
		}
	}
	z.mag.SetBytes32(raw[:])
	if z.mag.IsZero() {
		z.neg = false
	}
	return z, nil
}
