package protobuf

import (
	"errors"
	"fmt"
	"math"

	"google.golang.org/protobuf/encoding/protowire"
)

// ParseVarint parses varint-encoded uint64 from buf. Returns parsed value and
// number of bytes read.
func ParseVarint(buf []byte) (uint64, int, error) {
	u, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		// TODO: protowire package adds 'proto' prefix to error. We don't need it.
		return 0, 0, protowire.ParseError(n)
	}

	return u, n, nil
}

// ParseTag parses field tag from buf. Returns field number, type and number of
// bytes read.
func ParseTag(buf []byte) (protowire.Number, protowire.Type, int, error) {
	u, n, err := ParseVarint(buf)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse varint: %w", err)
	}

	num, typ := protowire.DecodeTag(u)
	if err = checkFieldNumber(num); err != nil {
		return 0, 0, 0, err
	}

	return num, typ, n, nil
}

// ParseLEN parses varint-encoded length from buf and check its overflow. Returns
// parsed value and number of bytes read.
func ParseLEN(buf []byte) (int, int, error) {
	ln, n, err := ParseVarint(buf)
	if err != nil {
		return 0, 0, fmt.Errorf("parse varint: %w", err)
	}

	if ln > math.MaxInt {
		return 0, 0, fmt.Errorf("value %d overflows int", ln)
	}

	if rem := len(buf) - n; int(ln) > rem {
		return 0, 0, newTruncatedBufferError(int(ln), rem)
	}

	return int(ln), n, nil
}

// ParseLENField parses length of LEN field with preread number and type from
// buf. Returns parsed value and number of bytes read.
//
// If there is an error, its text contains num and typ.
func ParseLENField(buf []byte, num protowire.Number, typ protowire.Type) (int, int, error) {
	err := checkFieldType(num, protowire.BytesType, typ)
	if err != nil {
		return 0, 0, err
	}

	ln, n, err := ParseLEN(buf)
	if err != nil {
		return 0, 0, wrapParseFieldError(num, protowire.BytesType, err)
	}

	return ln, n, nil
}

// ParseEnum parses enum value from buf. Returns parsed value and number of
// bytes read.
func ParseEnum[T ~int32](buf []byte) (T, int, error) {
	u, n, err := ParseVarint(buf)
	if err != nil {
		return 0, 0, fmt.Errorf("parse varint: %w", err)
	}

	if u > math.MaxInt32 {
		return 0, 0, fmt.Errorf("value %d overflows int32", u)
	}

	return T(u), n, nil
}

// ParseEnumField parses value of enum field with preread number and type from
// buf. Returns parsed value and number of bytes read.
//
// If there is an error, its text contains num and typ.
func ParseEnumField[T ~int32](buf []byte, num protowire.Number, typ protowire.Type) (T, int, error) {
	err := checkFieldType(num, protowire.VarintType, typ)
	if err != nil {
		return 0, 0, err
	}

	e, n, err := ParseEnum[T](buf)
	if err != nil {
		return 0, 0, wrapParseFieldError(num, protowire.VarintType, err)
	}

	return e, n, nil
}

// ParseUint32 parses varint-encoded uint32 from buf. Returns parsed value and
// number of bytes read.
func ParseUint32(buf []byte) (uint32, int, error) {
	u, n, err := ParseVarint(buf)
	if err != nil {
		return 0, 0, fmt.Errorf("parse varint: %w", err)
	}

	if u > math.MaxUint32 {
		return 0, 0, fmt.Errorf("value %d overflows uint32", u)
	}

	return uint32(u), n, nil
}

// ParseUint32Field parses value of uint32 field from buf. Returns parsed value
// and number of bytes read.
//
// If there is an error, its text contains num and typ.
func ParseUint32Field(buf []byte, num protowire.Number, typ protowire.Type) (uint32, int, error) {
	err := checkFieldType(num, protowire.VarintType, typ)
	if err != nil {
		return 0, 0, err
	}

	u, n, err := ParseUint32(buf)
	if err != nil {
		return 0, 0, wrapParseFieldError(num, protowire.VarintType, err)
	}

	return u, n, nil
}

// ParseUint64Field parses value of uint64 field with preread number and type
// from buf. Returns value and its length.
//
// If there is an error, its text contains num and typ.
func ParseUint64Field(buf []byte, num protowire.Number, typ protowire.Type) (uint64, int, error) {
	err := checkFieldType(num, protowire.VarintType, typ)
	if err != nil {
		return 0, 0, err
	}

	u, n, err := ParseVarint(buf)
	if err != nil {
		return 0, 0, wrapParseFieldError(num, protowire.VarintType, fmt.Errorf("parse varint: %w", err))
	}

	return u, n, nil
}

// SkipField parses length of skipped field with preread number and type from
// buf and checks its overflow. Returns number of bytes read.
//
// If there is an error, its text contains num and typ.
func SkipField(buf []byte, num protowire.Number, typ protowire.Type) (int, error) {
	var err error

	switch typ {
	case protowire.VarintType:
		var n int
		if _, n, err = ParseVarint(buf); err == nil {
			return n, nil
		}
	case protowire.Fixed64Type:
		if len(buf) >= fixed64Len {
			return fixed64Len, nil
		}
		err = newTruncatedBufferError(fixed64Len, len(buf))
	case protowire.BytesType:
		var ln, n int
		if ln, n, err = ParseLEN(buf); err == nil {
			return n + ln, nil
		}
	case protowire.StartGroupType, protowire.EndGroupType:
		err = errors.New("type is not supported")
	case protowire.Fixed32Type:
		if len(buf) >= fixed32Len {
			return fixed32Len, nil
		}
		err = newTruncatedBufferError(fixed32Len, len(buf))
	default:
		return 0, newUnknownFieldTypeError(typ)
	}

	return 0, wrapParseFieldError(num, typ, err)
}
