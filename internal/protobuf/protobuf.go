package protobuf

import (
	"errors"
	"fmt"
	"io"

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

// TODO: docs.
func SeekField(b []byte, num protowire.Number, typ protowire.Type) (int, error) {
	var off int

	for {
		gotNum, gotTyp, n := protowire.ConsumeTag(b[off:])
		if n < 0 {
			err := protowire.ParseError(n)
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return -1, nil
			}
			return 0, fmt.Errorf("read next field tag: %w", err)
		}

		if gotNum == num {
			if gotTyp != typ {
				return 0, fmt.Errorf("got field type %v instead of %v", gotTyp, typ)
			}
			return off, nil
		}

		off += n

		n = protowire.ConsumeFieldValue(gotNum, gotTyp, b[off:])
		if n < 0 {
			err := protowire.ParseError(n)
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return -1, nil
			}
			return 0, fmt.Errorf("skip value of field num=%v,type=%v: %w", gotNum, gotTyp, err)
		}

		off += n
	}
}
