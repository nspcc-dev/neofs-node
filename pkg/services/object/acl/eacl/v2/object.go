package v2

import (
	"strconv"

	"github.com/mr-tron/base58"
	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"google.golang.org/protobuf/encoding/protowire"
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

type binaryHeader []byte

func headersFromBinaryObjectHeader(b []byte, cnr cid.ID, id *oid.ID) ([]eaclSDK.Header, error) {
	res, err := _headersFromBinaryObjectHeader(b)
	if err != nil {
		return nil, err
	}

	if id != nil {
		return append(res, cidHeader(cnr), oidHeader(*id)), nil
	}

	return append(res, cidHeader(cnr)), nil
}

func _headersFromBinaryObjectHeader(b []byte) ([]eaclSDK.Header, error) {
	var ver version.Version
	var creationEpoch uint64
	var payloadLen uint64
	var objTyp object.Type
	res := make([]eaclSDK.Header, 0, 10)

	var off int
	var prevNum protowire.Number
	for {
		num, typ, n, err := iprotobuf.ParseTag(b, off)
		if err != nil {
			return nil, err
		}
		off += n

		if num < prevNum {
			return nil, iprotobuf.NewUnorderedFieldsError(prevNum, num)
		}
		if num == prevNum && num != iobject.FieldHeaderAttributes {
			return nil, iprotobuf.NewRepeatedFieldError(num)
		}
		prevNum = num

		switch num {
		case iobject.FieldHeaderVersion:
			ver, n, err = iprotobuf.ParseAPIVersionField(b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n
		case iobject.FieldHeaderContainerID:
			ln, n, err := iprotobuf.ParseLenField(b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n + ln
		case iobject.FieldHeaderOwnerID:
			owner, n, err := iprotobuf.ParseUserIDField(b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n

			res = append(res, sysObjHdr{k: eaclSDK.FilterObjectOwnerID, v: base58.Encode(owner)})
		case iobject.FieldHeaderCreationEpoch:
			creationEpoch, n, err = iprotobuf.ParseUint64Field(b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n
		case iobject.FieldHeaderPayloadLength:
			payloadLen, n, err = iprotobuf.ParseUint64Field(b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n
		case iobject.FieldHeaderPayloadHash:
			cs, n, err := iprotobuf.ParseChecksum(b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n

			res = append(res, sysObjHdr{k: eaclSDK.FilterObjectPayloadChecksum, v: cs.String()})
		case iobject.FieldHeaderType:
			objTyp, n, err = iprotobuf.ParseEnumField[object.Type](b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n
		case iobject.FieldHeaderHomoHash:
			cs, n, err := iprotobuf.ParseChecksum(b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n

			res = append(res, sysObjHdr{k: eaclSDK.FilterObjectPayloadHomomorphicChecksum, v: cs.String()})
		case iobject.FieldHeaderSessionToken:
			ln, n, err := iprotobuf.ParseLenField(b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n + ln
		case iobject.FieldHeaderAttributes:
			k, v, n, err := iprotobuf.ParseAttribute(b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n

			res = append(res, sysObjHdr{k: string(k), v: string(v)})
		case iobject.FieldHeaderSplit:
			ln, n, err := iprotobuf.ParseLenField(b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n

			parHdrf, err := iprotobuf.SeekBytesField(b[off:off+ln], iobject.FieldHeaderSplitParentHeader)
			if err != nil {
				return nil, iprotobuf.WrapParseFieldError(iobject.FieldHeaderSplit, protowire.BytesType, err)
			}

			if !parHdrf.IsMissing() {
				parRes, err := _headersFromBinaryObjectHeader(b[off:][parHdrf.ValueFrom:parHdrf.To])
				if err != nil {
					return nil, iprotobuf.WrapParseFieldError(iobject.FieldHeaderSplit, protowire.BytesType, err)
				}

				res = append(res, parRes...)
			}

			off += ln
		case iobject.FieldHeaderSessionTokenV2:
			ln, n, err := iprotobuf.ParseLenField(b, off, num, typ)
			if err != nil {
				return nil, err
			}
			off += n + ln
		default:
			return nil, iprotobuf.NewUnsupportedFieldError(num, typ)
		}

		if off == len(b) {
			break
		}
	}

	res = append(res,
		sysObjHdr{k: eaclSDK.FilterObjectCreationEpoch, v: u64Value(creationEpoch)},
		sysObjHdr{k: eaclSDK.FilterObjectPayloadSize, v: u64Value(payloadLen)},
		sysObjHdr{k: eaclSDK.FilterObjectVersion, v: ver.String()},
		sysObjHdr{k: eaclSDK.FilterObjectType, v: objTyp.String()},
	)

	return res, nil
}
