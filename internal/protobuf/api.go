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
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
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

// Common response field numbers.
const (
	FieldResponseBody               = 1
	FieldResponseMetaHeader         = 2
	FieldResponseVerificationHeader = 3
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

// VerifyAPIVersion checks whether buf is a valid NeoFS API version protobuf.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifyAPIVersion(buf []byte) error {
	_, err := VerifyAPIVersionWithOrder(buf)
	return err
}

// VerifyAPIVersion checks whether buf is a valid NeoFS API version protobuf. If
// so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifyAPIVersionWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protorefs.FieldVersionMajor, protorefs.FieldVersionMinor:
			if _, n, err = ParseUint32Field(buf[off:], num, typ); err != nil {
				return false, err
			}
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
		}

		off += n
	}

	return ordered, nil
}

// VerifyStatusDetail checks whether buf is a valid API status detail protobuf.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifyStatusDetail(buf []byte) error {
	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		off += n

		switch num {
		case protostatus.FieldStatusDetailID:
			if _, n, err = ParseUint32Field(buf[off:], num, typ); err != nil {
				return err
			}
		case protostatus.FieldStatusDetailValue:
			var ln int
			if ln, n, err = ParseLENField(buf[off:], num, typ); err != nil {
				return err
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return err
			}
		}

		off += n
	}
	return nil
}

// VerifyXHeader checks whether buf is a valid X-header protobuf.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifyXHeader(buf []byte) error {
	_, _, _, err := _verifyStringKeyValueWithOrder(buf)
	return err
}

// verifyObjectAttributeWithOrder checks whether buf is a valid object attribute
// protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func verifyObjectAttributeWithOrder(buf []byte) (bool, error) {
	k, v, ordered, err := _verifyStringKeyValueWithOrder(buf)
	if err != nil {
		return false, err
	}

	if len(k) == 0 {
		return false, errors.New("empty key")
	}

	if len(v) == 0 {
		return false, errors.New("empty value")
	}

	return ordered, nil
}

func _verifyStringKeyValueWithOrder(buf []byte) ([]byte, []byte, bool, error) {
	var k, v []byte

	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return nil, nil, false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protosession.FieldXHeaderKey:
			ln, n, err := ParseStringField(buf[off:], num, typ)
			if err != nil {
				return nil, nil, false, err
			}
			off += n
			k = buf[off:][:ln]
			off += ln
		case protosession.FieldXHeaderValue:
			ln, n, err := ParseStringField(buf[off:], num, typ)
			if err != nil {
				return nil, nil, false, err
			}
			off += n
			v = buf[off:][:ln]
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return nil, nil, false, err
			}
			off += n
		}
	}

	return k, v, ordered, nil
}

// VerifyObjectSplitInfo checks whether buf is a valid object split info
// protobuf.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifyObjectSplitInfo(buf []byte) error {
	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		off += n

		switch num {
		case protoobject.FieldSplitInfoSplitID:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return err
			}
			off += n
			if err = verifyUUIDV4Field(buf[off:][:ln]); err != nil {
				return fmt.Errorf("invalid split ID field: %w", err)
			}
			off += ln
		case protoobject.FieldSplitInfoLastPart, protoobject.FieldSplitInfoLink, protoobject.FieldSplitInfoFirstPart:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return err
			}
			off += n
			if err = VerifyObjectID(buf[off:][:ln]); err != nil {
				return fmt.Errorf("invalid object ID field #%d: %w", num, err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return err
			}
			off += n
		}
	}
	return nil
}

// VerifyObjectID checks whether buf is a valid object ID protobuf.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifyObjectID(buf []byte) error {
	_, err := VerifyObjectIDWithOrder(buf)
	return err
}

// VerifyObjectIDWithOrder checks whether buf is a valid object ID protobuf. If
// so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifyObjectIDWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protorefs.FieldObjectIDValue:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if err = verifyObjectIDValue(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid value field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// VerifyContainerIDWithOrder checks whether buf is a valid container ID
// protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifyContainerIDWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protorefs.FieldContainerIDValue:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if err = verifyContainerIDValue(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid value field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// VerifyContainerIDWithOrder checks whether buf is a valid user ID protobuf. If
// so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifyUserIDWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protorefs.FieldOwnerIDValue:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if err = verifyUserIDValue(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid value field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// VerifySignature checks whether buf is a valid signature protobuf.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifySignature(buf []byte) error {
	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		off += n

		switch num {
		case protorefs.FieldSignatureKey, protorefs.FieldSignatureValue:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return err
			}
			off += n + ln
		case protorefs.FieldSignatureScheme:
			if _, n, err = ParseEnumField[int32](buf[off:], num, typ); err != nil {
				return err
			}
			off += n
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return err
			}
			off += n
		}
	}
	return nil
}

// VerifySignatureWithOrder checks whether buf is a valid signature protobuf.If
// so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifySignatureWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protorefs.FieldSignatureKey, protorefs.FieldSignatureValue:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n + ln
		case protorefs.FieldSignatureScheme:
			if _, n, err = ParseEnumField[int32](buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// VerifyChecksumWithOrder checks whether buf is a valid checksum protobuf. If
// so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifyChecksumWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var withValue bool

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protorefs.FieldChecksumType:
			if _, n, err = ParseEnumField[int32](buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		case protorefs.FieldChecksumValue:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			if ln == 0 {
				return false, errors.New("empty value field")
			}
			withValue = true
			off += n + ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	if !withValue {
		return false, errors.New("missing value field")
	}

	return ordered, nil
}

// verifyTokenLifetimeWithOrder checks whether buf is a valid token lifetime
// protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func verifyTokenLifetimeWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		if num <= 3 {
			_, n, err = ParseUint64Field(buf[off:], num, typ)
			if err != nil {
				return false, fmt.Errorf("invalid uint64 field #%d: %w", num, err)
			}
		} else {
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
		}

		off += n
	}

	return ordered, nil
}

// verifySessionV1ObjectContextTarget checks whether buf is a valid session V1
// object context target protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func verifySessionV1ObjectContextTarget(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protosession.FieldObjectSessionContextTargetContainer:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyContainerIDWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid container ID field: %w", err)
			}
			off += ln
		case protosession.FieldObjectSessionContextTargetObjects:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyObjectIDWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid object ID field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// verifySessionV1ObjectContext checks whether buf is a valid session V1 object
// context protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func verifySessionV1ObjectContext(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protosession.FieldObjectSessionContextVerb:
			verb, n, err := ParseEnumField[int32](buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			if verb < 0 {
				return false, fmt.Errorf("invalid verb %d", verb)
			}
			off += n
		case protosession.FieldObjectSessionContextTarget:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = verifySessionV1ObjectContextTarget(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid target field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// verifySessionV1ContainerContext checks whether buf is a valid session V1
// container context protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func verifySessionV1ContainerContext(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protosession.FieldContainerSessionContextVerb:
			verb, n, err := ParseEnumField[int32](buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			if verb < 0 {
				return false, fmt.Errorf("invalid verb %d", verb)
			}
			off += n
		case protosession.FieldContainerSessionContextWildcard:
			if _, err = ParseBoolField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off++
		case protosession.FieldContainerSessionContextContainerID:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyContainerIDWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid container ID field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// verifySessionV1TokenBodyWithOrder checks whether buf is a valid session V1
// token body protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func verifySessionV1TokenBodyWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protosession.FieldSessionTokenBodyID:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if err = verifyUUIDV4Field(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid ID field: %w", err)
			}
			off += ln
		case protosession.FieldSessionTokenBodyOwnerID:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyUserIDWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid owner field: %w", err)
			}
			off += ln
		case protosession.FieldSessionTokenBodyLifetime:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = verifyTokenLifetimeWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid lifetime field: %w", err)
			}
			off += ln
		case protosession.FieldSessionTokenBodySessionKey:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			if ln == 0 {
				return false, errors.New("empty session key field")
			}
			off += n + ln
		case protosession.FieldSessionTokenBodyObject:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = verifySessionV1ObjectContext(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid object context field: %w", err)
			}
			off += ln
		case protosession.FieldSessionTokenBodyContainer:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = verifySessionV1ContainerContext(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid container context field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// VerifyChecksumWithOrder checks whether buf is a valid session V1 token
// protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifySessionV1TokenWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protosession.FieldSessionTokenBody:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = verifySessionV1TokenBodyWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid body field: %w", err)
			}
			off += ln
		case protosession.FieldSessionTokenSignature:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifySignatureWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid signature field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// verifySessionV1TokenBodyWithOrder checks whether buf is a valid session token
// subject protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func verifySessionTokenSubjectWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protosession.FieldTargetOwnerID:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyUserIDWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid owner field: %w", err)
			}
			off += ln
		case protosession.FieldTargetNNSName:
			ln, n, err := ParseStringField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n + ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// verifySessionTokenContextWithOrder checks whether buf is a valid session
// token context protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func verifySessionTokenContextWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protosession.FieldSessionContextV2Container:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyContainerIDWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid container field: %w", err)
			}
			off += ln
		case protosession.FieldSessionContextV2Verbs:
			if n, err = SkipRepeatedEnum(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// verifySessionTokenBodyWithOrder checks whether buf is a valid session token
// body protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func verifySessionTokenBodyWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protosession.FieldSessionTokenV2BodyVersion:
			if _, n, err = ParseUint32Field(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		case protosession.FieldSessionTokenV2BodyAppdata:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += ln + n
		case protosession.FieldSessionTokenV2BodyIssuer:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyUserIDWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid issuer field: %w", err)
			}
			off += ln
		case protosession.FieldSessionTokenV2BodySubjects:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = verifySessionTokenSubjectWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid subject field: %w", err)
			}
			off += ln
		case protosession.FieldSessionTokenV2BodyLifetime:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = verifyTokenLifetimeWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid lifetime field: %w", err)
			}
			off += ln
		case protosession.FieldSessionTokenV2BodyContexts:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = verifySessionTokenContextWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid context field: %w", err)
			}
			off += ln
		case protosession.FieldSessionTokenV2BodyFinal:
			if _, err = ParseBoolField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off++
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// VerifySessionTokenWithOrder checks whether buf is a valid session token
// protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifySessionTokenWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protosession.FieldSessionTokenV2Body:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = verifySessionTokenBodyWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid body field: %w", err)
			}
			off += ln
		case protosession.FieldSessionTokenV2Signature:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifySignatureWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid signature field: %w", err)
			}
			off += ln
		case protosession.FieldSessionTokenV2Origin:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifySessionTokenWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid origin field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// verifySplitHeaderWithOrder checks whether buf is a valid split header
// protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func verifySplitHeaderWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protoobject.FieldHeaderSplitParent,
			protoobject.FieldHeaderSplitPrevious,
			protoobject.FieldHeaderSplitFirst,
			protoobject.FieldHeaderSplitChildren:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyObjectIDWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid object ID field #%d: %w", num, err)
			}
			off += ln
		case protoobject.FieldHeaderSplitParentHeader:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyObjectHeaderWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid parent header field: %w", err)
			}
			off += ln
		case protoobject.FieldHeaderSplitParentSignature:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifySignatureWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid parent signature field: %w", err)
			}
			off += ln
		case protoobject.FieldHeaderSplitSplitID:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if err = verifyUUIDV4Field(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid split ID field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// VerifyObjectHeaderWithOrder checks whether buf is a valid object header
// protobuf. If so, direct field order flag is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func VerifyObjectHeaderWithOrder(buf []byte) (bool, error) {
	ordered := true
	var prevNum protowire.Number

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return false, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		if ordered {
			if num < prevNum {
				ordered = false
			} else {
				prevNum = num
			}
		}

		off += n

		switch num {
		case protoobject.FieldHeaderVersion:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyAPIVersionWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid version field: %w", err)
			}
			off += ln
		case protoobject.FieldHeaderContainerID:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyContainerIDWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid container ID field: %w", err)
			}
			off += ln
		case protoobject.FieldHeaderOwnerID:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyUserIDWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid owner ID field: %w", err)
			}
			off += ln
		case protoobject.FieldHeaderCreationEpoch, protoobject.FieldHeaderPayloadLength:
			if _, n, err = ParseUint64Field(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		case protoobject.FieldHeaderPayloadHash, protoobject.FieldHeaderHomomorphicHash:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifyChecksumWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid checksum field #%d: %w", num, err)
			}
			off += ln
		case protoobject.FieldHeaderObjectType:
			if _, n, err = ParseEnumField[int32](buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		case protoobject.FieldHeaderSessionToken:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifySessionV1TokenWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid session V1 token field: %w", err)
			}
			off += ln
		case protoobject.FieldHeaderAttributes:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = verifyObjectAttributeWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid attribute field: %w", err)
			}
			off += ln
		case protoobject.FieldHeaderSplit:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = verifySplitHeaderWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid split field: %w", err)
			}
			off += ln
		case protoobject.FieldHeaderSessionV2:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return false, err
			}
			off += n
			if ordered, err = VerifySessionTokenWithOrder(buf[off:][:ln]); err != nil {
				return false, fmt.Errorf("invalid session field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return false, err
			}
			off += n
		}
	}

	return ordered, nil
}

// GetStatusCodeFromResponseMetaHeader checks whether buf is a valid response
// meta header protobuf. If so, status code field is returned. In case of
// nesting headers, code from the root is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed: if status field is repeated (including nested),
// code from the last one is returned.
func GetStatusCodeFromResponseMetaHeader(buf []byte) (uint32, error) {
	var originFld []byte
	var statusFld []byte

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return 0, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		off += n

		switch num {
		case protosession.FieldResponseMetaHeaderVersion:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return 0, err
			}
			off += n
			if err = VerifyAPIVersion(buf[off:][:ln]); err != nil {
				return 0, fmt.Errorf("invalid version field: %w", err)
			}
			off += ln
		case protosession.FieldResponseMetaHeaderEpoch:
			if _, n, err = ParseUint64Field(buf[off:], num, typ); err != nil {
				return 0, err
			}
			off += n
		case protosession.FieldResponseMetaHeaderTTL:
			if _, n, err = ParseUint32Field(buf[off:], num, typ); err != nil {
				return 0, err
			}
			off += n
		case protosession.FieldResponseMetaHeaderXHeaders:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return 0, err
			}
			off += n
			if err = VerifyXHeader(buf[off:][:ln]); err != nil {
				return 0, fmt.Errorf("invalid X-header field: %w", err)
			}
			off += ln
		case protosession.FieldResponseMetaHeaderOrigin:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return 0, err
			}
			off += n
			originFld = buf[off:][:ln]
			off += ln
		case protosession.FieldResponseMetaHeaderStatus:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return 0, err
			}
			off += n
			statusFld = buf[off:][:ln]
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return 0, err
			}
			off += n
		}
	}

	if originFld != nil {
		return GetStatusCodeFromResponseMetaHeader(originFld)
	}

	code, err := getCodeFromStatus(statusFld)
	if err != nil {
		return 0, fmt.Errorf("invalid status field: %w", err)
	}

	return code, nil
}

// getCodeFromStatus checks whether buf is a valid response status protobuf. If
// so, code field is returned.
//
// Absense of any fields is ignored. Unknown fields are allowed and checked.
// Repeating fields is allowed.
func getCodeFromStatus(buf []byte) (uint32, error) {
	var code uint32

	var off int
	for len(buf[off:]) > 0 {
		num, typ, n, err := ParseTag(buf[off:])
		if err != nil {
			return 0, fmt.Errorf("parse tag at offset %d: %w", off, err)
		}

		off += n

		switch num {
		case protostatus.FieldStatusCode:
			code, n, err = ParseUint32Field(buf[off:], num, typ)
			if err != nil {
				return 0, err
			}
			off += n
		case protostatus.FieldStatusMessage:
			ln, n, err := ParseStringField(buf[off:], num, typ)
			if err != nil {
				return 0, err
			}
			off += n + ln
		case protostatus.FieldStatusDetails:
			ln, n, err := ParseLENField(buf[off:], num, typ)
			if err != nil {
				return 0, err
			}
			off += n
			if err = VerifyStatusDetail(buf[off:][:ln]); err != nil {
				return 0, fmt.Errorf("invalid details field: %w", err)
			}
			off += ln
		default:
			if n, err = SkipField(buf[off:], num, typ); err != nil {
				return 0, err
			}
			off += n
		}
	}

	return code, nil
}
