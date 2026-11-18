package v2

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/mr-tron/base58"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	iprotobuf "github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"google.golang.org/protobuf/encoding/protowire"
)

// reservedAttributePrefix separates system object properties from user attributes.
const reservedAttributePrefix = "$Object:"

func lookupObjectHeader(obj *object.Object, cnr cid.ID, id oid.ID, addressOnly bool, key string) (string, bool) {
	if obj != nil || addressOnly {
		switch key {
		case eacl.FilterObjectContainerID:
			return cnr.EncodeToString(), true
		case eacl.FilterObjectID:
			if !id.IsZero() {
				return id.EncodeToString(), true
			}
			return "", false
		}
	}

	if strings.HasPrefix(key, reservedAttributePrefix) {
		for ; obj != nil; obj = obj.Parent() {
			if value, ok := systemObjectHeader(obj, key); ok {
				return value, true
			}
		}
	} else {
		for ; obj != nil; obj = obj.Parent() {
			for _, attr := range obj.Attributes() {
				if attr.Key() == key {
					return attr.Value(), true
				}
			}
		}
	}
	return "", false
}

func systemObjectHeader(obj *object.Object, key string) (string, bool) {
	switch key {
	case eacl.FilterObjectCreationEpoch:
		return strconv.FormatUint(obj.CreationEpoch(), 10), true
	case eacl.FilterObjectPayloadSize:
		return strconv.FormatUint(obj.PayloadSize(), 10), true
	case eacl.FilterObjectVersion:
		if ver := obj.Version(); ver != nil {
			return ver.String(), true
		}
		return "v0.0", true
	case eacl.FilterObjectType:
		return obj.Type().String(), true
	case object.FilterExpirationTime:
		if expiration := obj.ExpirationTime(); !expiration.IsZero() {
			return strconv.FormatInt(expiration.Unix(), 10), true
		}
	case eacl.FilterObjectOwnerID:
		if owner := obj.Owner(); !owner.IsZero() {
			return owner.EncodeToString(), true
		}
	case eacl.FilterObjectPayloadChecksum:
		if cs, ok := obj.PayloadChecksum(); ok {
			return cs.String(), true
		}
	case eacl.FilterObjectPayloadHomomorphicChecksum:
		//nolint:staticcheck // if we store old objects with it, we should support old eACL rules
		if cs, ok := obj.PayloadHomomorphicHash(); ok {
			return cs.String(), true
		}
	}
	return "", false
}

// lookupBinaryObjectHeader prefers the current header over its split parent.
// The first lookup validates the whole header, including any split parents.
func lookupBinaryObjectHeader(buf []byte, cnr cid.ID, id oid.ID, key string, validate bool) ([]byte, error) {
	var ver version.Version
	var creationEpoch uint64
	var payloadLen uint64
	var objTyp object.Type
	var value, parentValue []byte
	system := strings.HasPrefix(key, reservedAttributePrefix)

	var off int
	for {
		num, typ, n, err := iprotobuf.ParseTag(buf[off:])
		if err != nil {
			return nil, err
		}

		off += n

		switch num {
		case protoobject.FieldHeaderVersion:
			ver, n, err = iprotobuf.ParseAPIVersionField(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n
		case protoobject.FieldHeaderContainerID:
			ln, n, err := iprotobuf.ParseLENField(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n + ln
		case protoobject.FieldHeaderOwnerID:
			owner, n, err := iprotobuf.ParseUserIDField(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n

			if value == nil && key == eacl.FilterObjectOwnerID {
				value = []byte(base58.Encode(owner))
			}
		case protoobject.FieldHeaderCreationEpoch:
			creationEpoch, n, err = iprotobuf.ParseUint64Field(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n
		case protoobject.FieldHeaderPayloadLength:
			payloadLen, n, err = iprotobuf.ParseUint64Field(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n
		case protoobject.FieldHeaderPayloadHash:
			cs, n, err := iprotobuf.ParseChecksum(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n

			if value == nil && key == eacl.FilterObjectPayloadChecksum {
				value = []byte(cs.String())
			}
		case protoobject.FieldHeaderObjectType:
			objTyp, n, err = iprotobuf.ParseEnumField[object.Type](buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n
		case protoobject.FieldHeaderHomomorphicHash:
			cs, n, err := iprotobuf.ParseChecksum(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n

			//nolint:staticcheck // if we store old objects with it, we should support old eACL rules
			if value == nil && key == eacl.FilterObjectPayloadHomomorphicChecksum {
				value = []byte(cs.String())
			}
		case protoobject.FieldHeaderSessionToken:
			ln, n, err := iprotobuf.ParseLENField(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n + ln
		case protoobject.FieldHeaderAttributes:
			if !validate {
				attr, n := protowire.ConsumeBytes(buf[off:])
				off += n
				if !system {
					if v := lookupValidatedAttribute(attr, key); v != nil {
						return v, nil
					}
				}
				break
			}
			k, v, n, err := iprotobuf.ParseAttribute(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n

			if value == nil && !system && string(k) == key {
				value = v
				if value == nil {
					value = []byte{}
				}
			}
		case protoobject.FieldHeaderSplit:
			ln, n, err := iprotobuf.ParseLENField(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n

			parHdrf, err := iprotobuf.GetLENFieldBounds(buf[off:][:ln], protoobject.FieldHeaderSplitParentHeader)
			if err != nil {
				return nil, fmt.Errorf("invalid split header field: %w", err)
			}

			if !parHdrf.IsMissing() {
				v, err := lookupBinaryObjectHeader(buf[off:][parHdrf.ValueFrom:parHdrf.To], cnr, id, key, validate)
				if err != nil {
					return nil, fmt.Errorf("invalid split header field: %w", err)
				}
				if parentValue == nil {
					parentValue = v
				}
			}

			off += ln
		case protoobject.FieldHeaderSessionV2:
			ln, n, err := iprotobuf.ParseLENField(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n + ln
		case protoobject.FieldHeaderExpirationTime:
			expiration, n, err := iprotobuf.ParseUint64Field(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n
			if value == nil && key == object.FilterExpirationTime {
				value = strconv.AppendUint(nil, expiration, 10)
			}
		default:
			return nil, iprotobuf.NewUnsupportedFieldError(num, typ)
		}

		if off == len(buf) {
			break
		}
	}

	if value != nil {
		return value, nil
	}
	switch key {
	case eacl.FilterObjectCreationEpoch:
		return strconv.AppendUint(nil, creationEpoch, 10), nil
	case eacl.FilterObjectPayloadSize:
		return strconv.AppendUint(nil, payloadLen, 10), nil
	case eacl.FilterObjectVersion:
		return []byte(ver.String()), nil
	case eacl.FilterObjectType:
		return []byte(objTyp.String()), nil
	case eacl.FilterObjectContainerID:
		return []byte(cnr.EncodeToString()), nil
	case eacl.FilterObjectID:
		if !id.IsZero() {
			return []byte(id.EncodeToString()), nil
		}
	}
	return parentValue, nil
}

// lookupValidatedAttribute relies on ParseAttribute having checked field order,
// types and lengths. It only reads the value if the key matches.
func lookupValidatedAttribute(buf []byte, key string) []byte {
	if len(buf) == 0 {
		if key == "" {
			return []byte{}
		}
		return nil
	}
	num, _, n := protowire.ConsumeTag(buf)
	if num == protoobject.FieldAttributeKey {
		k, size := protowire.ConsumeBytes(buf[n:])
		if string(k) != key {
			return nil
		}
		buf = buf[n+size:]
		if len(buf) == 0 {
			return []byte{}
		}
		_, _, n = protowire.ConsumeTag(buf)
	} else if key != "" {
		return nil
	}
	v, _ := protowire.ConsumeBytes(buf[n:])
	return v
}
