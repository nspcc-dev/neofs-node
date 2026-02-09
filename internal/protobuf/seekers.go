package protobuf

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// SeekFieldByNumber seeks field in buf by number and returns its offset, tag
// length and type. If field is missing, negative offset returns.
//
// Message should have ascending field order, otherwise error returns.
//
// Note that SeekFieldByNumber does not check value of found field, but checks
// intermediate ones for correct message traverse.
func SeekFieldByNumber(buf []byte, seekNum protowire.Number) (int, int, protowire.Type, error) {
	if err := checkFieldNumber(seekNum); err != nil {
		return 0, 0, 0, err
	}

	if len(buf) == 0 {
		return -1, 0, 0, nil
	}

	var off int
	var prevNum protowire.Number

	for {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return 0, 0, 0, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if num == seekNum {
			return off, n, typ, nil
		}
		if num > seekNum {
			break
		}
		if num < prevNum {
			return 0, 0, 0, NewUnorderedFieldsError(prevNum, num)
		}
		prevNum = num

		off += n

		n, err = SkipField(buf[off:], num, typ)
		if err != nil {
			return 0, 0, 0, err
		}
		off += n

		if off == len(buf) {
			break
		}
	}

	return -1, 0, 0, nil
}

// GetLENFieldBounds seeks LEN field in buf by number and parses its boundaries.
// If field is missing, no error is returned.
//
// Message should have ascending field order, otherwise error returns.
//
// If there is an error, its text contains num.
func GetLENFieldBounds(buf []byte, num protowire.Number) (FieldBounds, error) {
	off, tagLn, typ, err := SeekFieldByNumber(buf, num)
	if err != nil {
		return FieldBounds{}, err
	}

	if off < 0 {
		return FieldBounds{}, nil
	}

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
