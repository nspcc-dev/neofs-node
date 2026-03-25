package protobuf

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	fixed32Len = 4
	fixed64Len = 8

	uuidLen = 16
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

func verifyContainerIDValue(b []byte) error {
	if len(b) != cid.Size {
		return fmt.Errorf("invalid len: %d instead of %d", len(b), cid.Size)
	}
	if islices.AllZeros(b) {
		return cid.ErrZero
	}
	return nil
}

func verifyObjectIDValue(b []byte) error {
	if len(b) != oid.Size {
		return fmt.Errorf("invalid len: %d instead of %d", len(b), oid.Size)
	}
	if islices.AllZeros(b) {
		return oid.ErrZero
	}
	return nil
}

func verifyUserIDValue(b []byte) error {
	if len(b) != user.IDSize {
		return fmt.Errorf("invalid len: %d instead of %d", len(b), user.IDSize)
	}
	if b[0] != address.NEO3Prefix {
		return fmt.Errorf("invalid prefix byte 0x%X, expected 0x%X", b[0], address.NEO3Prefix)
	}
	if !bytes.Equal(b[21:], hash.Checksum(b[:21])) {
		return errors.New("checksum mismatch")
	}
	return nil
}

func verifyUUIDV4Field(b []byte) error {
	if len(b) != uuidLen {
		return fmt.Errorf("invalid len: %d instead of %d", len(b), uuidLen)
	}
	if ver := b[6] >> 4; ver != 4 {
		return fmt.Errorf("wrong UUID version %d, expected 4", ver)
	}
	return nil
}
