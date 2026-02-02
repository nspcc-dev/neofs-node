package protobuf

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"google.golang.org/protobuf/encoding/protowire"
)

// Various field numbers.
const (
	FieldVersionMajor = 1
	FieldVersionMinor = 2

	FieldUserIDValue = 1

	FieldChecksumType  = 1
	FieldChecksumValue = 2

	FieldAttributeKey   = 1
	FieldAttributeValue = 2
)

// ParseAPIVersionField parses version.Version from the next field with known
// number and type at given offset. Also returns field length.
func ParseAPIVersionField(b []byte, off int, num protowire.Number, typ protowire.Type) (version.Version, int, error) {
	lnFull, nFull, err := ParseLenField(b, off, num, typ)
	if err != nil || lnFull == 0 {
		return version.Version{}, nFull, err
	}
	off += nFull

	b = b[:off+lnFull]

	var ver version.Version
	var prevNum protowire.Number
	for {
		num, typ, n, err := ParseTag(b, off)
		if err != nil {
			return version.Version{}, 0, err
		}
		off += n

		if num < prevNum {
			return version.Version{}, 0, NewUnorderedFieldsError(prevNum, num)
		}
		if num == prevNum {
			return version.Version{}, 0, NewRepeatedFieldError(num)
		}
		prevNum = num

		switch num {
		case FieldVersionMajor:
			u, n, err := ParseUint32Field(b, off, num, typ)
			if err != nil {
				return version.Version{}, 0, err
			}
			off += n

			ver.SetMajor(u)
		case FieldVersionMinor:
			u, n, err := ParseUint32Field(b, off, num, typ)
			if err != nil {
				return version.Version{}, 0, err
			}
			off += n

			ver.SetMinor(u)
		default:
			return version.Version{}, 0, NewUnsupportedFieldError(num, typ)
		}

		if off == len(b) {
			break
		}
	}

	return ver, nFull + lnFull, nil
}

// ParseUserIDField parses user ID from the next field with known number and
// type at given offset. Also returns field length.
func ParseUserIDField(b []byte, off int, num protowire.Number, typ protowire.Type) ([]byte, int, error) {
	lnFull, nFull, err := ParseLenField(b, off, num, typ)
	if err != nil || lnFull == 0 {
		return nil, nFull, err
	}
	off += nFull

	b = b[:off+lnFull]

	num, typ, n, err := ParseTag(b, off)
	if err != nil {
		return nil, 0, err
	}
	off += n

	if num != FieldUserIDValue {
		return nil, 0, NewUnsupportedFieldError(num, typ)
	}

	ln, n, err := ParseLenField(b, off, num, typ)
	if err != nil {
		return nil, 0, err
	}
	off += n

	b = b[off : off+ln]

	// TODO https://github.com/nspcc-dev/neofs-sdk-go/issues/669
	switch {
	case len(b) != user.IDSize:
		return nil, 0, fmt.Errorf("invalid length %d, expected %d", len(b), user.IDSize)
	case b[0] != address.NEO3Prefix:
		return nil, 0, fmt.Errorf("invalid prefix byte 0x%X, expected 0x%X", b[0], address.NEO3Prefix)
	case !bytes.Equal(b[21:], hash.Checksum(b[:21])):
		return nil, 0, errors.New("checksum mismatch")
	}

	return b, nFull + lnFull, nil
}

// ParseChecksum parses checksum.Checksum from the next field with known number
// and type at given offset. Also returns field length.
func ParseChecksum(b []byte, off int, num protowire.Number, typ protowire.Type) (checksum.Checksum, int, error) {
	lnFull, nFull, err := ParseLenField(b, off, num, typ)
	if err != nil || lnFull == 0 {
		return checksum.Checksum{}, nFull, err
	}
	off += nFull

	b = b[:off+lnFull]

	var csTyp checksum.Type
	var csVal []byte
	var prevNum protowire.Number
	for {
		num, typ, n, err := ParseTag(b, off)
		if err != nil {
			return checksum.Checksum{}, 0, err
		}
		off += n

		if num < prevNum {
			return checksum.Checksum{}, 0, NewUnorderedFieldsError(prevNum, num)
		}
		if num == prevNum {
			return checksum.Checksum{}, 0, NewRepeatedFieldError(num)
		}
		prevNum = num

		switch num {
		case FieldChecksumType:
			csTyp, n, err = ParseEnumField[checksum.Type](b, off, num, typ)
			if err != nil {
				return checksum.Checksum{}, 0, err
			}
			off += n
		case FieldChecksumValue:
			ln, n, err := ParseLenField(b, off, num, typ)
			if err != nil {
				return checksum.Checksum{}, 0, err
			}
			off += n

			csVal = b[off : off+ln]

			off += ln
		default:
			return checksum.Checksum{}, 0, NewUnsupportedFieldError(num, typ)
		}

		if off == len(b) {
			break
		}
	}

	return checksum.New(csTyp, csVal), nFull + lnFull, nil
}

// ParseChecksum parses key-value attribute from the next field with known
// number and type at given offset. Also returns field length.
func ParseAttribute(b []byte, off int, num protowire.Number, typ protowire.Type) ([]byte, []byte, int, error) {
	lnFull, nFull, err := ParseLenField(b, off, num, typ)
	if err != nil || lnFull == 0 {
		return nil, nil, nFull, err
	}
	off += nFull

	b = b[:off+lnFull]

	var k, v []byte
	var prevNum protowire.Number
	for {
		num, typ, n, err := ParseTag(b, off)
		if err != nil {
			return nil, nil, 0, err
		}
		off += n

		if num < prevNum {
			return nil, nil, 0, NewUnorderedFieldsError(prevNum, num)
		}
		if num == prevNum {
			return nil, nil, 0, NewRepeatedFieldError(num)
		}
		prevNum = num

		switch num {
		case FieldAttributeKey:
			ln, n, err := ParseLenField(b, off, num, typ)
			if err != nil {
				return nil, nil, 0, err
			}
			off += n

			k = b[off : off+ln]

			off += ln
		case FieldAttributeValue:
			ln, n, err := ParseLenField(b, off, num, typ)
			if err != nil {
				return nil, nil, 0, err
			}
			off += n

			v = b[off : off+ln]

			off += ln
		default:
			return nil, nil, 0, NewUnsupportedFieldError(num, typ)
		}

		if off == len(b) {
			break
		}
	}

	return k, v, nFull + lnFull, nil
}
