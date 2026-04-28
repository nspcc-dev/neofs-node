package v2

import (
	"fmt"
	"strconv"

	"github.com/mr-tron/base58"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/version"
)

type sysObjHdr struct {
	k, v string
}

func (s sysObjHdr) Key() string {
	return s.k
}

func (s sysObjHdr) Value() string {
	return s.v
}

func u64Value(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func headersFromObject(obj *object.Object, cnr cid.ID, oid *oid.ID) []eaclSDK.Header {
	var count int
	for obj := obj; obj != nil; obj = obj.Parent() {
		count += 9 + len(obj.Attributes())
	}

	res := make([]eaclSDK.Header, 0, count)
	for ; obj != nil; obj = obj.Parent() {
		var ver = obj.Version()
		if ver == nil {
			ver = &version.Version{}
		}
		res = append(res,
			cidHeader(cnr),
			// creation epoch
			sysObjHdr{
				k: eaclSDK.FilterObjectCreationEpoch,
				v: u64Value(obj.CreationEpoch()),
			},
			// payload size
			sysObjHdr{
				k: eaclSDK.FilterObjectPayloadSize,
				v: u64Value(obj.PayloadSize()),
			},
			// object version
			sysObjHdr{
				k: eaclSDK.FilterObjectVersion,
				v: ver.String(),
			},
			// object type
			sysObjHdr{
				k: eaclSDK.FilterObjectType,
				v: obj.Type().String(),
			},
		)

		if oid != nil {
			res = append(res, oidHeader(*oid))
		}

		if idOwner := obj.Owner(); !idOwner.IsZero() {
			res = append(res, ownerIDHeader(idOwner))
		}

		cs, ok := obj.PayloadChecksum()
		if ok {
			res = append(res, sysObjHdr{
				k: eaclSDK.FilterObjectPayloadChecksum,
				v: cs.String(),
			})
		}

		//nolint:staticcheck // if we store old objects with it, we should support old eACL rules
		cs, ok = obj.PayloadHomomorphicHash()
		if ok {
			res = append(res, sysObjHdr{
				k: eaclSDK.FilterObjectPayloadHomomorphicChecksum,
				v: cs.String(),
			})
		}

		attrs := obj.Attributes()
		for i := range attrs {
			res = append(res, &attrs[i]) // only pointer attrs can implement eaclSDK.Header interface
		}
	}

	return res
}

func headersFromBinaryObjectHeader(buf []byte, cnr cid.ID, id *oid.ID) ([]eaclSDK.Header, error) {
	var ver version.Version
	var creationEpoch uint64
	var payloadLen uint64
	var objTyp object.Type
	res := make([]eaclSDK.Header, 0, 10)

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

			res = append(res, sysObjHdr{k: eaclSDK.FilterObjectOwnerID, v: base58.Encode(owner)})
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

			res = append(res, sysObjHdr{k: eaclSDK.FilterObjectPayloadChecksum, v: cs.String()})
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
			res = append(res, sysObjHdr{k: eaclSDK.FilterObjectPayloadHomomorphicChecksum, v: cs.String()})
		case protoobject.FieldHeaderSessionToken:
			ln, n, err := iprotobuf.ParseLENField(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n + ln
		case protoobject.FieldHeaderAttributes:
			k, v, n, err := iprotobuf.ParseAttribute(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n

			res = append(res, sysObjHdr{k: string(k), v: string(v)})
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
				parRes, err := headersFromBinaryObjectHeader(buf[off:][parHdrf.ValueFrom:parHdrf.To], cnr, id)
				if err != nil {
					return nil, fmt.Errorf("invalid split header field: %w", err)
				}

				res = append(res, parRes...)
			}

			off += ln
		case protoobject.FieldHeaderSessionV2:
			ln, n, err := iprotobuf.ParseLENField(buf[off:], num, typ)
			if err != nil {
				return nil, err
			}
			off += n + ln
		default:
			return nil, iprotobuf.NewUnsupportedFieldError(num, typ)
		}

		if off == len(buf) {
			break
		}
	}

	res = append(res,
		sysObjHdr{k: eaclSDK.FilterObjectCreationEpoch, v: u64Value(creationEpoch)},
		sysObjHdr{k: eaclSDK.FilterObjectPayloadSize, v: u64Value(payloadLen)},
		sysObjHdr{k: eaclSDK.FilterObjectVersion, v: ver.String()},
		sysObjHdr{k: eaclSDK.FilterObjectType, v: objTyp.String()},
		cidHeader(cnr),
	)

	if id != nil {
		res = append(res, oidHeader(*id))
	}

	return res, nil
}
