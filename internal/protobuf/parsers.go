package protobuf

import (
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protowire"
)

// ParseTag parses tag of the next field in at given offset. Returns field
// number, type and tag length.
func ParseTag(b []byte, off int) (protowire.Number, protowire.Type, int, error) {
	num, typ, n := protowire.ConsumeTag(b[off:])
	if n < 0 {
		return 0, 0, 0, fmt.Errorf("parse field tag: %w", protowire.ParseError(n))
	}

	return num, typ, n, nil
}

func parseVarint(b []byte, off int) (uint64, int, error) {
	u, n := protowire.ConsumeVarint(b[off:])
	if n < 0 {
		return 0, 0, fmt.Errorf("parse varint: %w", protowire.ParseError(n))
	}

	return u, n, nil
}

// ParseLen parses length of the varlen field at given offset. Returns length of
// varlen tag and the length itself.
func ParseLen(b []byte, off int) (int, int, error) {
	ln, n, err := parseVarint(b, off)
	if err != nil {
		return 0, 0, fmt.Errorf("parse field len: %w", err)
	}

	if rem := uint64(len(b) - off - n); ln > rem {
		return 0, 0, fmt.Errorf("parse field len: %w (got %d, left buffer %d)", io.ErrUnexpectedEOF, ln, rem)
	}

	return int(ln), n, nil
}

// ParseLen parses length of the next varlen field with known number and type at
// given offset. Returns length of varlen tag and the length itself.
func ParseLenField(b []byte, off int, num protowire.Number, typ protowire.Type) (int, int, error) {
	err := checkFieldType(num, protowire.BytesType, typ)
	if err != nil {
		return 0, 0, err
	}

	ln, n, err := ParseLen(b, off)
	if err != nil {
		return 0, 0, WrapParseFieldError(num, protowire.BytesType, err)
	}

	return ln, n, nil
}

// ParseLenFieldBounds parses length of the next varlen field with tag length,
// number and type at given offset.
func ParseLenFieldBounds(b []byte, off int, tagLn int, num protowire.Number, typ protowire.Type) (FieldBounds, error) {
	ln, nLn, err := ParseLenField(b, off+tagLn, num, typ)
	if err != nil {
		return FieldBounds{}, err
	}

	var f FieldBounds
	f.From = off
	f.ValueFrom = f.From + tagLn + nLn
	f.To = f.ValueFrom + ln

	return f, nil
}

// ParseAnyField parses value of the next field with known number and type at
// given offset. Returns value length.
func ParseAnyField(b []byte, off int, num protowire.Number, typ protowire.Type) (int, error) {
	// TODO: can be optimized by calculating len only?
	n := protowire.ConsumeFieldValue(num, typ, b[off:])
	if n < 0 {
		return 0, WrapParseFieldError(num, typ, protowire.ParseError(n))
	}

	return n, nil
}
