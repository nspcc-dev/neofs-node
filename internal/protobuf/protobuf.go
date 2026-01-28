package protobuf

import (
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protowire"
)

// Fixed protobuf field tags.
const (
	// TagBytes1 = 10
	TagBytes2 = 18
	TagBytes3 = 26
)

// TODO: docs.
type FieldBounds struct {
	From      int
	ValueFrom int
	To        int
}

// GetFirstBytesField gets VARLEN field with number = 1 from b.
//
// GetFirstBytesField returns slice of b, not copy.
func GetFirstBytesField(b []byte) ([]byte, error) {
	fNum, fTyp, n := protowire.ConsumeTag(b)
	if n < 0 {
		return nil, fmt.Errorf("parse first field tag: %w", protowire.ParseError(n))
	}

	if fNum != 1 {
		return nil, fmt.Errorf("first field num is %d instead of 1", fNum)
	}

	if fTyp != protowire.BytesType {
		return nil, fmt.Errorf("first field type is %v instead of %v", fTyp, protowire.BytesType)
	}

	b, n = protowire.ConsumeBytes(b[n:])
	if n < 0 {
		return nil, fmt.Errorf("parse bytes field: %w", protowire.ParseError(n))
	}

	return b, nil
}

// TODO: docs.
func SeekBytesField(b []byte, num protowire.Number) (FieldBounds, error) {
	off, tagLn, err := seekField(b, num, protowire.BytesType)
	if err != nil {
		return FieldBounds{}, err
	}

	if off < 0 {
		return FieldBounds{From: off}, nil
	}

	return ParseBytesFieldBounds(b, off, tagLn)
}

// TODO: docs.
func ParseBytesFieldBounds(b []byte, off int, tagLn int) (FieldBounds, error) {
	ln, lnLen := protowire.ConsumeVarint(b[off+tagLn:])
	if err := protowire.ParseError(lnLen); err != nil {
		return FieldBounds{}, fmt.Errorf("parse len: %w", err)
	}

	if rem := uint64(len(b) - off - tagLn - lnLen); ln > rem {
		return FieldBounds{}, fmt.Errorf("%w (len %d, left %d)", io.ErrUnexpectedEOF, ln, rem)
	}

	var f FieldBounds
	f.From = off
	f.ValueFrom = f.From + tagLn + lnLen
	f.To = f.ValueFrom + int(ln)

	return f, nil
}

func seekField(b []byte, seekNum protowire.Number, seekTyp protowire.Type) (int, int, error) {
	var off int
	var prevNum protowire.Number
	for {
		num, typ, tagLn := protowire.ConsumeTag(b[off:])
		if err := protowire.ParseError(tagLn); err != nil {
			return 0, 0, WrapParseFieldTagError(err)
		}

		if num == seekNum {
			if typ != seekTyp {
				return 0, 0, NewWrongFieldTypeError(typ)
			}

			return off, tagLn, nil
		}

		if num > seekNum {
			break
		}

		if num < prevNum {
			return 0, 0, NewUnorderedFieldsError(prevNum, num)
		}
		prevNum = num

		ln := protowire.ConsumeFieldValue(num, typ, b[off+tagLn:])
		if err := protowire.ParseError(ln); err != nil {
			return 0, 0, WrapParseFieldError(num, seekTyp, err)
		}

		off += tagLn + ln

		if off == len(b) {
			break
		}
	}

	return -1, 0, nil
}
