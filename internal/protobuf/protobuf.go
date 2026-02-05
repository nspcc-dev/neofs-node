package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

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

// SeekBytesField seeks varlen field type by number.
func SeekBytesField(b []byte, num protowire.Number) (FieldBounds, error) {
	off, tagLn, typ, err := seekField(b, num)
	if err != nil {
		return FieldBounds{}, WrapSeekFieldError(num, err)
	}

	if off < 0 {
		return FieldBounds{}, nil
	}

	return ParseLenFieldBounds(b, off, tagLn, num, typ)
}

func seekField(b []byte, seekNum protowire.Number) (int, int, protowire.Type, error) {
	var off int
	var prevNum protowire.Number
	for {
		num, typ, n, err := ParseTag(b, off)
		if err != nil {
			return 0, 0, 0, err
		}

		if num == seekNum {
			return off, n, typ, nil
		}
		if num < prevNum {
			return 0, 0, 0, NewUnorderedFieldsError(prevNum, num)
		}
		if num > seekNum {
			break
		}
		prevNum = num

		off += n

		n, err = ParseAnyField(b, off, num, typ)
		if err != nil {
			return 0, 0, 0, err
		}
		off += n

		if off == len(b) {
			break
		}
	}

	return -1, 0, 0, nil
}
