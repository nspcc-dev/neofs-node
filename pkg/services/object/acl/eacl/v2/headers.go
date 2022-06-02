package v2

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	refsV2 "github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type Option func(*cfg)

type cfg struct {
	storage ObjectStorage

	msg xHeaderSource

	cnr cid.ID
	obj *oid.ID
}

type ObjectStorage interface {
	Head(oid.Address) (*object.Object, error)
}

type Request interface {
	GetMetaHeader() *session.RequestMetaHeader
}

type Response interface {
	GetMetaHeader() *session.ResponseMetaHeader
}

type headerSource struct {
	requestHeaders []eaclSDK.Header
	objectHeaders  []eaclSDK.Header

	incompleteObjectHeaders bool
}

func defaultCfg() *cfg {
	return &cfg{
		storage: new(localStorage),
	}
}

func NewMessageHeaderSource(opts ...Option) (eaclSDK.TypedHeaderSource, error) {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	if cfg.msg == nil {
		return nil, errors.New("message is not provided")
	}

	var res headerSource

	err := cfg.readObjectHeaders(&res)
	if err != nil {
		return nil, err
	}

	res.requestHeaders = requestHeaders(cfg.msg)

	return res, nil
}

func (h headerSource) HeadersOfType(typ eaclSDK.FilterHeaderType) ([]eaclSDK.Header, bool) {
	switch typ {
	default:
		return nil, true
	case eaclSDK.HeaderFromRequest:
		return h.requestHeaders, true
	case eaclSDK.HeaderFromObject:
		return h.objectHeaders, !h.incompleteObjectHeaders
	}
}

type xHeader session.XHeader

func (x xHeader) Key() string {
	return (*session.XHeader)(&x).GetKey()
}

func (x xHeader) Value() string {
	return (*session.XHeader)(&x).GetValue()
}

func requestHeaders(msg xHeaderSource) []eaclSDK.Header {
	return msg.GetXHeaders()
}

var errMissingOID = errors.New("object ID is missing")

func (h *cfg) readObjectHeaders(dst *headerSource) error {
	switch m := h.msg.(type) {
	default:
		panic(fmt.Sprintf("unexpected message type %T", h.msg))
	case requestXHeaderSource:
		switch req := m.req.(type) {
		case
			*objectV2.GetRequest,
			*objectV2.HeadRequest:
			if h.obj == nil {
				return errMissingOID
			}

			var addr oid.Address
			addr.SetContainer(h.cnr)
			addr.SetObject(*h.obj)

			obj, err := h.storage.Head(addr)
			if err == nil {
				dst.objectHeaders = headersFromObject(obj, h.cnr, h.obj)
			} else {
				dst.incompleteObjectHeaders = true
			}
		case
			*objectV2.GetRangeRequest,
			*objectV2.GetRangeHashRequest,
			*objectV2.DeleteRequest:
			if h.obj == nil {
				return errMissingOID
			}

			dst.objectHeaders = addressHeaders(h.cnr, h.obj)
		case *objectV2.PutRequest:
			if v, ok := req.GetBody().GetObjectPart().(*objectV2.PutObjectPartInit); ok {
				oV2 := new(objectV2.Object)
				oV2.SetObjectID(v.GetObjectID())
				oV2.SetHeader(v.GetHeader())

				dst.objectHeaders = headersFromObject(object.NewFromV2(oV2), h.cnr, h.obj)
			}
		case *objectV2.SearchRequest:
			cnrV2 := req.GetBody().GetContainerID()
			var cnr cid.ID

			if cnrV2 != nil {
				if err := cnr.ReadFromV2(*cnrV2); err != nil {
					return fmt.Errorf("can't parse container ID: %w", err)
				}
			}

			dst.objectHeaders = []eaclSDK.Header{cidHeader(cnr)}
		}
	case responseXHeaderSource:
		switch resp := m.resp.(type) {
		case *objectV2.GetResponse:
			if v, ok := resp.GetBody().GetObjectPart().(*objectV2.GetObjectPartInit); ok {
				oV2 := new(objectV2.Object)
				oV2.SetObjectID(v.GetObjectID())
				oV2.SetHeader(v.GetHeader())

				dst.objectHeaders = headersFromObject(object.NewFromV2(oV2), h.cnr, h.obj)
			}
		case *objectV2.HeadResponse:
			oV2 := new(objectV2.Object)

			var hdr *objectV2.Header

			switch v := resp.GetBody().GetHeaderPart().(type) {
			case *objectV2.ShortHeader:
				hdr = new(objectV2.Header)

				var idV2 refsV2.ContainerID
				h.cnr.WriteToV2(&idV2)

				hdr.SetContainerID(&idV2)
				hdr.SetVersion(v.GetVersion())
				hdr.SetCreationEpoch(v.GetCreationEpoch())
				hdr.SetOwnerID(v.GetOwnerID())
				hdr.SetObjectType(v.GetObjectType())
				hdr.SetPayloadLength(v.GetPayloadLength())
			case *objectV2.HeaderWithSignature:
				hdr = v.GetHeader()
			}

			oV2.SetHeader(hdr)

			dst.objectHeaders = headersFromObject(object.NewFromV2(oV2), h.cnr, h.obj)
		}
	}

	return nil
}

func cidHeader(idCnr cid.ID) sysObjHdr {
	return sysObjHdr{
		k: acl.FilterObjectContainerID,
		v: idCnr.EncodeToString(),
	}
}

func oidHeader(obj oid.ID) sysObjHdr {
	return sysObjHdr{
		k: acl.FilterObjectID,
		v: obj.EncodeToString(),
	}
}

func ownerIDHeader(ownerID user.ID) sysObjHdr {
	return sysObjHdr{
		k: acl.FilterObjectOwnerID,
		v: ownerID.EncodeToString(),
	}
}

func addressHeaders(cnr cid.ID, oid *oid.ID) []eaclSDK.Header {
	hh := make([]eaclSDK.Header, 0, 2)
	hh = append(hh, cidHeader(cnr))

	if oid != nil {
		hh = append(hh, oidHeader(*oid))
	}

	return hh
}
