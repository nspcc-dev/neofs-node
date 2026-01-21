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
