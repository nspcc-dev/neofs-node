package protobuf

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"

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

// ParseVarint parses following varint-encoded uint64 number.
//
// If no error, positions x right after the number.
func (x *BuffersSlice) parseVarint() (uint64, error) {
	// implementation is based on binary.Uvarint which is iterative unlike protowire.ConsumeVarint.
	var u uint64
	var s uint
	var i int
	for b := range x.bytesSeq {
		if i == binary.MaxVarintLen64 {
			// Catch byte reads past MaxVarintLen64.
			// See issue https://golang.org/issues/41185
			return 0, errVarintOverflow
		}
		if b < 0x80 {
			if i == binary.MaxVarintLen64-1 && b > 1 {
				return 0, errVarintOverflow // overflow
			}
			return u | uint64(b)<<s, nil
		}
		u |= uint64(b&0x7f) << s
		s += 7
		i++
	}
	return 0, io.ErrUnexpectedEOF
}

func _parseTag(u uint64, err error) (protowire.Number, protowire.Type, error) {
	if err != nil {
		return 0, 0, fmt.Errorf("parse varint: %w", err)
	}

	num, typ := protowire.DecodeTag(u)
	if err = checkFieldNumber(num); err != nil {
		return 0, 0, err
	}

	return num, typ, nil
}

// ParseTag parses field tag from buf. Returns field number, type and number of
// bytes read.
func ParseTag(buf []byte) (protowire.Number, protowire.Type, int, error) {
	u, n, err := ParseVarint(buf)
	num, typ, err := _parseTag(u, err)
	if err != nil {
		return 0, 0, 0, err
	}

	return num, typ, n, nil
}

// ParseTag parses following field tag.
//
// If no error, positions x right after the tag.
func (x *BuffersSlice) ParseTag() (protowire.Number, protowire.Type, error) {
	return _parseTag(x.parseVarint())
}

func _checkLEN(u uint64, err error) (int, error) {
	if err != nil {
		return 0, fmt.Errorf("parse varint: %w", err)
	}

	if u > math.MaxInt {
		return 0, fmt.Errorf("value %d overflows int", u)
	}

	return int(u), nil
}

// ParseLEN parses varint-encoded length from buf and check its overflow. Returns
// parsed value and number of bytes read.
func ParseLEN(buf []byte) (int, int, error) {
	u, n, err := ParseVarint(buf)
	ln, err := _checkLEN(u, err)
	if err != nil {
		return 0, 0, err
	}

	if rem := len(buf) - n; ln > rem {
		return 0, 0, newTruncatedBufferError(ln, rem)
	}

	return ln, n, nil
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

// ParseLENField parses following bytes or nested message field with preread
// number and type.
//
// If no error, positions x right after the field.
//
// If there is an error, its text contains num and typ.
func (x *BuffersSlice) ParseLENField(num protowire.Number, typ protowire.Type) (BuffersSlice, error) {
	err := checkFieldType(num, protowire.BytesType, typ)
	if err != nil {
		return BuffersSlice{}, err
	}

	ln, err := _checkLEN(x.parseVarint())
	if err != nil {
		return BuffersSlice{}, wrapParseFieldError(num, protowire.BytesType, err)
	}

	sub, err := x.MoveNext(ln)
	if err != nil {
		return BuffersSlice{}, wrapParseFieldError(num, protowire.BytesType, err)
	}

	return sub, nil
}

// ParseLENFieldBounds parses boundaries of LEN field with preread tag length,
// number and type at given offset from buf.
//
// If there is an error, its text contains num and typ.
func ParseLENFieldBounds(buf []byte, off int, tagLn int, num protowire.Number, typ protowire.Type) (FieldBounds, error) {
	ln, n, err := ParseLENField(buf[off+tagLn:], num, typ)
	if err != nil {
		return FieldBounds{}, err
	}

	var f FieldBounds
	f.From = off
	f.ValueFrom = f.From + tagLn + n
	f.To = f.ValueFrom + ln

	return f, nil
}

// ParseStringField parses following string field with preread number and type.
//
// If no error, positions x right after the field.
//
// If there is an error, its text contains num and typ.
func (x *BuffersSlice) ParseStringField(num protowire.Number, typ protowire.Type) ([]byte, error) {
	sub, err := x.ParseLENField(num, typ)
	if err != nil {
		return nil, err
	}

	s := sub.ReadOnlyData()
	if !utf8.Valid(s) {
		return nil, fmt.Errorf("string field #%d contains invalid UTF-8", num)
	}

	return s, nil
}

func _checkEnum[T ~int32](u uint64, err error) (T, error) {
	if err != nil {
		return 0, fmt.Errorf("parse varint: %w", err)
	}

	if u > math.MaxInt32 {
		return 0, fmt.Errorf("value %d overflows int32", u)
	}

	return T(u), nil
}

// ParseEnum parses enum value from buf. Returns parsed value and number of
// bytes read.
func ParseEnum[T ~int32](buf []byte) (T, int, error) {
	u, n, err := ParseVarint(buf)
	t, err := _checkEnum[T](u, err)
	if err != nil {
		return 0, 0, err
	}

	return t, n, nil
}

// [ParseEnum] analogue for scanning.
func (x *BuffersSlice) parseEnum() (int32, error) {
	return _checkEnum[int32](x.parseVarint())
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

// ParseEnumField parses following enum field with preread number and type.
//
// If no error, positions x right after the field.
//
// If there is an error, its text contains num and typ.
func (x *BuffersSlice) ParseEnumField(num protowire.Number, typ protowire.Type) (int32, error) {
	err := checkFieldType(num, protowire.VarintType, typ)
	if err != nil {
		return 0, err
	}

	e, err := x.parseEnum()
	if err != nil {
		return 0, wrapParseFieldError(num, protowire.VarintType, err)
	}

	return e, nil
}

func _checkUint32(u uint64, err error) (uint32, error) {
	if err != nil {
		return 0, fmt.Errorf("parse varint: %w", err)
	}

	if u > math.MaxUint32 {
		return 0, fmt.Errorf("value %d overflows uint32", u)
	}

	return uint32(u), nil
}

// ParseUint32 parses varint-encoded uint32 from buf. Returns parsed value and
// number of bytes read.
func ParseUint32(buf []byte) (uint32, int, error) {
	u, n, err := ParseVarint(buf)
	u32, err := _checkUint32(u, err)
	if err != nil {
		return 0, 0, err
	}

	return u32, n, nil
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

// ParseUint32Field parses following uint32 field with preread number and type.
//
// If no error, positions x right after the field.
//
// If there is an error, its text contains num and typ.
func (x *BuffersSlice) ParseUint32Field(num protowire.Number, typ protowire.Type) (uint32, error) {
	err := checkFieldType(num, protowire.VarintType, typ)
	if err != nil {
		return 0, err
	}

	u, err := _checkUint32(x.parseVarint())
	if err != nil {
		return 0, wrapParseFieldError(num, protowire.VarintType, err)
	}

	return u, nil
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

// ParseUint64Field parses following uint64 field with preread number and type.
//
// If no error, positions x right after the field.
//
// If there is an error, its text contains num and typ.
func (x *BuffersSlice) ParseUint64Field(num protowire.Number, typ protowire.Type) (uint64, error) {
	err := checkFieldType(num, protowire.VarintType, typ)
	if err != nil {
		return 0, err
	}

	u, err := x.parseVarint()
	if err != nil {
		return 0, wrapParseFieldError(num, protowire.VarintType, fmt.Errorf("parse varint: %w", err))
	}

	return u, nil
}

// ParseBoolField parses following bool field with preread number and type.
//
// If no error, positions x right after the field.
//
// If there is an error, its text contains num and typ.
func (x *BuffersSlice) ParseBoolField(num protowire.Number, typ protowire.Type) (bool, error) {
	err := checkFieldType(num, protowire.VarintType, typ)
	if err != nil {
		return false, err
	}

	u, err := x.parseVarint()
	if err != nil {
		return false, err
	}

	if u > 1 {
		return false, fmt.Errorf("invalid varint value for bool field %d", u)
	}

	return u == 1, nil
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

// SkipRepeatedEnum verifies following repeated enum field with preread number
// and type.
//
// If no error, positions x right after the field.
//
// If there is an error, its text contains num and typ.
func (x *BuffersSlice) SkipRepeatedEnum(num protowire.Number, typ protowire.Type) error {
	sub, err := x.ParseLENField(num, typ)
	if err != nil {
		return err
	}

	for !sub.IsEmpty() {
		if _, err = sub.parseEnum(); err != nil {
			return fmt.Errorf("parse next element: %w", err)
		}
	}

	return nil
}
