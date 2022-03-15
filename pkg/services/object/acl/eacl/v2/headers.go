package v2

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objectSDKAddress "github.com/nspcc-dev/neofs-sdk-go/object/address"
	objectSDKID "github.com/nspcc-dev/neofs-sdk-go/object/id"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
)

type Option func(*cfg)

type cfg struct {
	storage ObjectStorage

	msg xHeaderSource

	addr *objectSDKAddress.Address
}

type ObjectStorage interface {
	Head(*objectSDKAddress.Address) (*object.Object, error)
}

type Request interface {
	GetMetaHeader() *session.RequestMetaHeader
}

type Response interface {
	GetMetaHeader() *session.ResponseMetaHeader
}

type headerSource struct {
	*cfg
}

func defaultCfg() *cfg {
	return &cfg{
		storage: new(localStorage),
	}
}

func NewMessageHeaderSource(opts ...Option) eaclSDK.TypedHeaderSource {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	return &headerSource{
		cfg: cfg,
	}
}

func (h *headerSource) HeadersOfType(typ eaclSDK.FilterHeaderType) ([]eaclSDK.Header, bool) {
	switch typ {
	default:
		return nil, true
	case eaclSDK.HeaderFromRequest:
		return requestHeaders(h.msg), true
	case eaclSDK.HeaderFromObject:
		return h.objectHeaders()
	}
}

func requestHeaders(msg xHeaderSource) []eaclSDK.Header {
	xHdrs := msg.GetXHeaders()

	res := make([]eaclSDK.Header, 0, len(xHdrs))

	for i := range xHdrs {
		res = append(res, sessionSDK.NewXHeaderFromV2(&xHdrs[i]))
	}

	return res
}

func (h *headerSource) objectHeaders() ([]eaclSDK.Header, bool) {
	switch m := h.msg.(type) {
	default:
		panic(fmt.Sprintf("unexpected message type %T", h.msg))
	case *requestXHeaderSource:
		switch req := m.req.(type) {
		case *objectV2.GetRequest:
			return h.localObjectHeaders(h.addr)
		case *objectV2.HeadRequest:
			return h.localObjectHeaders(h.addr)
		case
			*objectV2.GetRangeRequest,
			*objectV2.GetRangeHashRequest,
			*objectV2.DeleteRequest:
			return addressHeaders(h.addr), true
		case *objectV2.PutRequest:
			if v, ok := req.GetBody().GetObjectPart().(*objectV2.PutObjectPartInit); ok {
				oV2 := new(objectV2.Object)
				oV2.SetObjectID(v.GetObjectID())
				oV2.SetHeader(v.GetHeader())

				if h.addr == nil {
					addr := objectSDKAddress.NewAddress()
					addr.SetContainerID(cid.NewFromV2(v.GetHeader().GetContainerID()))
					addr.SetObjectID(objectSDKID.NewIDFromV2(v.GetObjectID()))
				}

				hs := headersFromObject(object.NewFromV2(oV2), h.addr)

				return hs, true
			}
		case *objectV2.SearchRequest:
			return []eaclSDK.Header{cidHeader(
				cid.NewFromV2(
					req.GetBody().GetContainerID()),
			)}, true
		}
	case *responseXHeaderSource:
		switch resp := m.resp.(type) {
		default:
			hs, _ := h.localObjectHeaders(h.addr)
			return hs, true
		case *objectV2.GetResponse:
			if v, ok := resp.GetBody().GetObjectPart().(*objectV2.GetObjectPartInit); ok {
				oV2 := new(objectV2.Object)
				oV2.SetObjectID(v.GetObjectID())
				oV2.SetHeader(v.GetHeader())

				return headersFromObject(object.NewFromV2(oV2), h.addr), true
			}
		case *objectV2.HeadResponse:
			oV2 := new(objectV2.Object)

			var hdr *objectV2.Header

			switch v := resp.GetBody().GetHeaderPart().(type) {
			case *objectV2.ShortHeader:
				hdr = new(objectV2.Header)

				hdr.SetContainerID(h.addr.ContainerID().ToV2())
				hdr.SetVersion(v.GetVersion())
				hdr.SetCreationEpoch(v.GetCreationEpoch())
				hdr.SetOwnerID(v.GetOwnerID())
				hdr.SetObjectType(v.GetObjectType())
				hdr.SetPayloadLength(v.GetPayloadLength())
			case *objectV2.HeaderWithSignature:
				hdr = v.GetHeader()
			}

			oV2.SetHeader(hdr)

			return headersFromObject(object.NewFromV2(oV2), h.addr), true
		}
	}

	return nil, true
}

func (h *headerSource) localObjectHeaders(addr *objectSDKAddress.Address) ([]eaclSDK.Header, bool) {
	obj, err := h.storage.Head(addr)
	if err == nil {
		return headersFromObject(obj, addr), true
	}

	return addressHeaders(addr), false
}

func cidHeader(idCnr *cid.ID) eaclSDK.Header {
	return &sysObjHdr{
		k: acl.FilterObjectContainerID,
		v: cidValue(idCnr),
	}
}

func oidHeader(oid *objectSDKID.ID) eaclSDK.Header {
	return &sysObjHdr{
		k: acl.FilterObjectID,
		v: idValue(oid),
	}
}

func addressHeaders(addr *objectSDKAddress.Address) []eaclSDK.Header {
	res := make([]eaclSDK.Header, 1, 2)
	res[0] = cidHeader(addr.ContainerID())

	if oid := addr.ObjectID(); oid != nil {
		res = append(res, oidHeader(oid))
	}

	return res
}
