package protobuf

import (
	"fmt"
	"io"
	"strconv"

	"google.golang.org/protobuf/encoding/protowire"
)

const (
	fixed32Len = 4
	fixed64Len = 8
)

type wireType protowire.Type

func (x wireType) String() string {
	switch protowire.Type(x) {
	case protowire.VarintType:
		return "VARINT"
	case protowire.Fixed64Type:
		return "I64"
	case protowire.BytesType:
		return "LEN"
	case protowire.StartGroupType:
		return "SGROUP"
	case protowire.EndGroupType:
		return "EGROUP"
	case protowire.Fixed32Type:
		return "I32"
	default:
		return strconv.Itoa(int(x))
	}
}

func checkFieldType(num protowire.Number, exp, got protowire.Type) error {
	if exp != got {
		return fmt.Errorf("wrong type of field #%d: expected %s, got %s", num, wireType(exp), wireType(got))
	}
	return nil
}

func checkFieldNumber(num protowire.Number) error {
	if !num.IsValid() {
		return fmt.Errorf("invalid number %d", num)
	}
	return nil
}

func newUnknownFieldTypeError(t protowire.Type) error {
	return fmt.Errorf("unknown field type %s", wireType(t))
}

func newTruncatedBufferError(need, left int) error {
	return fmt.Errorf("%w: need %d bytes, left %d in buffer", io.ErrUnexpectedEOF, need, left)
}

func wrapParseFieldError(n protowire.Number, t protowire.Type, cause error) error {
	return fmt.Errorf("parse field #%d of %s type: %w", n, wireType(t), cause)
}
