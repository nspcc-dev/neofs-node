package protobuf

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	protorefs "github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"google.golang.org/protobuf/encoding/protowire"
)

// Fixed message lengths.
const (
	ObjectIDLength = 1 + 1 + oid.Size
)

// Message length limits.
const (
	MaxSignatureLength = 1 + 2 + neofscrypto.MaxVerificationScriptLength +
		1 + 2 + neofscrypto.MaxInvocationScriptLength +
		1 + 5 // scheme tag + varint max int32
	MaxObjectWithoutPayloadLength = 1 + 1 + ObjectIDLength +
		1 + 2 + MaxSignatureLength +
		1 + 3 + object.MaxHeaderLen
)

// ParseAPIVersionField parses version.Version from the next field with known
// number and type at given offset. Also returns field length.
func ParseAPIVersionField(buf []byte, fNum protowire.Number, fTyp protowire.Type) (version.Version, int, error) {
	lnf, nf, err := ParseLENField(buf, fNum, fTyp)
	if err != nil || lnf == 0 {
		return version.Version{}, nf, err
	}

	buf = buf[nf:][:lnf]

	var ver version.Version
	var off int
	var prevNum protowire.Number
	for {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return version.Version{}, 0, err
		}

		if num < prevNum {
			return version.Version{}, 0, NewUnorderedFieldsError(prevNum, num)
		}
		if num == prevNum {
			return version.Version{}, 0, NewRepeatedFieldError(num)
		}
		if num != protorefs.FieldVersionMajor && num != protorefs.FieldVersionMinor {
			return version.Version{}, 0, NewUnsupportedFieldError(num, typ)
		}
		prevNum = num

		off += n

		u, n, err := ParseUint32Field(buf[off:], num, typ)
		if err != nil {
			return version.Version{}, 0, err
		}

		switch num {
		case protorefs.FieldVersionMajor:
			ver.SetMajor(u)
		case protorefs.FieldVersionMinor:
			ver.SetMinor(u)
		default:
			panic("unreachable with num " + strconv.Itoa(int(num)))
		}

		off += n

		if off == len(buf) {
			break
		}
	}

	return ver, nf + lnf, nil
}

// ParseUserIDField parses user ID from the next field with known number and
// type at given offset. Also returns field length.
func ParseUserIDField(buf []byte, fNum protowire.Number, fTyp protowire.Type) ([]byte, int, error) {
	lnf, nf, err := ParseLENField(buf, fNum, fTyp)
	if err != nil || lnf == 0 {
		return nil, nf, err
	}

	buf = buf[nf:][:lnf]

	num, typ, n, err := ParseTag(buf)
	if err != nil {
		return nil, 0, err
	}
	if num != protorefs.FieldOwnerIDValue {
		return nil, 0, NewUnsupportedFieldError(num, typ)
	}

	off := n

	ln, n, err := ParseLENField(buf[off:], num, typ)
	if err != nil {
		return nil, 0, err
	}

	off += n

	buf = buf[off:][:ln]

	// TODO https://github.com/nspcc-dev/neofs-sdk-go/issues/669
	switch {
	case len(buf) != user.IDSize:
		return nil, 0, fmt.Errorf("invalid length %d, expected %d", len(buf), user.IDSize)
	case buf[0] != address.NEO3Prefix:
		return nil, 0, fmt.Errorf("invalid prefix byte 0x%X, expected 0x%X", buf[0], address.NEO3Prefix)
	case !bytes.Equal(buf[21:], hash.Checksum(buf[:21])):
		return nil, 0, errors.New("checksum mismatch")
	}

	return buf, nf + lnf, nil
}

// ParseChecksum parses checksum.Checksum from the next field with known number
// and type at given offset. Also returns field length.
func ParseChecksum(buf []byte, fNum protowire.Number, fTyp protowire.Type) (checksum.Checksum, int, error) {
	lnf, nf, err := ParseLENField(buf, fNum, fTyp)
	if err != nil || lnf == 0 {
		return checksum.Checksum{}, nf, err
	}

	buf = buf[nf:][:lnf]

	var csTyp checksum.Type
	var csVal []byte
	var off int
	var prevNum protowire.Number
	for {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return checksum.Checksum{}, 0, err
		}

		if num < prevNum {
			return checksum.Checksum{}, 0, NewUnorderedFieldsError(prevNum, num)
		}
		if num == prevNum {
			return checksum.Checksum{}, 0, NewRepeatedFieldError(num)
		}
		prevNum = num

		off += n

		switch num {
		case protorefs.FieldChecksumType:
			typ, n, err := ParseEnumField[protorefs.ChecksumType](buf[off:], num, typ)
			if err != nil {
				return checksum.Checksum{}, 0, err
			}
			switch typ {
			case protorefs.ChecksumType_SHA256:
				csTyp = checksum.SHA256
			case protorefs.ChecksumType_TZ:
				csTyp = checksum.TillichZemor
			default:
				csTyp = checksum.Type(typ)
			}
			off += n
		case protorefs.FieldChecksumValue:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return checksum.Checksum{}, 0, err
			}
			off += n
			csVal = buf[off:][:ln]
			off += ln
		default:
			return checksum.Checksum{}, 0, NewUnsupportedFieldError(num, typ)
		}

		if off == len(buf) {
			break
		}
	}

	return checksum.New(csTyp, csVal), nf + lnf, nil
}

// ParseChecksum parses key-value attribute from the next field with known
// number and type at given offset. Also returns field length.
func ParseAttribute(buf []byte, fNum protowire.Number, fTyp protowire.Type) ([]byte, []byte, int, error) {
	lnf, nf, err := ParseLENField(buf, fNum, fTyp)
	if err != nil || lnf == 0 {
		return nil, nil, nf, err
	}

	buf = buf[nf:][:lnf]

	var k, v []byte
	var off int
	var prevNum protowire.Number
	for {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return nil, nil, 0, err
		}

		if num < prevNum {
			return nil, nil, 0, NewUnorderedFieldsError(prevNum, num)
		}
		if num == prevNum {
			return nil, nil, 0, NewRepeatedFieldError(num)
		}
		if num != protoobject.FieldAttributeKey && num != protoobject.FieldAttributeValue {
			return nil, nil, 0, NewUnsupportedFieldError(num, typ)
		}
		prevNum = num

		off += n

		ln, n, err := ParseLENField(buf[off:], num, typ)
		if err != nil {
			return nil, nil, 0, err
		}

		off += n

		switch num {
		case protoobject.FieldAttributeKey:
			k = buf[off:][:ln]
		case protorefs.FieldVersionMinor:
			v = buf[off:][:ln]
		default:
			panic("unreachable with num " + strconv.Itoa(int(num)))
		}

		off += ln

		if off == len(buf) {
			break
		}
	}

	return k, v, nf + lnf, nil
}
