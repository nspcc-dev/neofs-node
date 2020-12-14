package v2

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	eaclSDK "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/acl/eacl"
)

type Option func(*cfg)

type cfg struct {
	storage ObjectStorage

	msg xHeaderSource
}

type ObjectStorage interface {
	Head(*objectSDK.Address) (*object.Object, error)
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

func NewMessageHeaderSource(opts ...Option) eacl.TypedHeaderSource {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	return &headerSource{
		cfg: cfg,
	}
}

func (h *headerSource) HeadersOfType(typ eaclSDK.FilterHeaderType) ([]eacl.Header, bool) {
	switch typ {
	default:
		return nil, true
	case eaclSDK.HeaderFromRequest:
		return requestHeaders(h.msg), true
	case eaclSDK.HeaderFromObject:
		return h.objectHeaders()
	}
}

func requestHeaders(msg xHeaderSource) []eacl.Header {
	xHdrs := msg.GetXHeaders()

	res := make([]eacl.Header, 0, len(xHdrs))

	for i := range xHdrs {
		res = append(res, pkg.NewXHeaderFromV2(xHdrs[i]))
	}

	return res
}

func (h *headerSource) objectHeaders() ([]eacl.Header, bool) {
	switch m := h.msg.(type) {
	default:
		panic(fmt.Sprintf("unexpected message type %T", h.msg))
	case *requestXHeaderSource:
		switch req := m.req.(type) {
		case *objectV2.GetRequest:
			return h.localObjectHeaders(req.GetBody().GetAddress())
		case *objectV2.DeleteRequest:
			hs, _ := h.localObjectHeaders(req.GetBody().GetAddress())
			return hs, true
		case *objectV2.HeadRequest:
			return h.localObjectHeaders(req.GetBody().GetAddress())
		case *objectV2.GetRangeRequest:
			return addressHeaders(objectSDK.NewAddressFromV2(req.GetBody().GetAddress())), true
		case *objectV2.GetRangeHashRequest:
			hs, _ := h.localObjectHeaders(req.GetBody().GetAddress())
			return hs, true
		case *objectV2.PutRequest:
			if v, ok := req.GetBody().GetObjectPart().(*objectV2.PutObjectPartInit); ok {
				oV2 := new(objectV2.Object)
				oV2.SetObjectID(v.GetObjectID())
				oV2.SetHeader(v.GetHeader())

				hs := headersFromObject(object.NewFromV2(oV2))
				if tok := oV2.GetHeader().GetSessionToken(); tok != nil {
					objCtx, ok := tok.GetBody().GetContext().(*session.ObjectSessionContext)
					if ok {
						hs = append(hs, addressHeaders(objectSDK.NewAddressFromV2(objCtx.GetAddress()))...)
					}
				}

				return hs, true
			}
		case *objectV2.SearchRequest:
			return []eacl.Header{cidHeader(
				container.NewIDFromV2(
					req.GetBody().GetContainerID()),
			)}, true
		}
	case *responseXHeaderSource:
		switch resp := m.resp.(type) {
		default:
			hs, _ := h.localObjectHeaders(m.addr)
			return hs, true
		case *objectV2.GetResponse:
			if v, ok := resp.GetBody().GetObjectPart().(*objectV2.GetObjectPartInit); ok {
				oV2 := new(objectV2.Object)
				oV2.SetObjectID(v.GetObjectID())
				oV2.SetHeader(v.GetHeader())

				return headersFromObject(object.NewFromV2(oV2)), true
			}
		case *objectV2.HeadResponse:
			oV2 := new(objectV2.Object)

			var hdr *objectV2.Header

			switch v := resp.GetBody().GetHeaderPart().(type) {
			case *objectV2.ShortHeader:
				hdr = new(objectV2.Header)

				hdr.SetContainerID(m.addr.GetContainerID())
				hdr.SetVersion(v.GetVersion())
				hdr.SetCreationEpoch(v.GetCreationEpoch())
				hdr.SetOwnerID(v.GetOwnerID())
				hdr.SetObjectType(v.GetObjectType())
				hdr.SetPayloadLength(v.GetPayloadLength())
			case *objectV2.HeaderWithSignature:
				hdr = v.GetHeader()
			}

			oV2.SetHeader(hdr)

			return append(
				headersFromObject(object.NewFromV2(oV2)),
				oidHeader(objectSDK.NewIDFromV2(m.addr.GetObjectID())),
			), true
		}
	}

	return nil, true
}

func (h *headerSource) localObjectHeaders(addrV2 *refs.Address) ([]eacl.Header, bool) {
	addr := objectSDK.NewAddressFromV2(addrV2)

	obj, err := h.storage.Head(addr)
	if err == nil {
		return append(headersFromObject(obj), addressHeaders(addr)...), true
	}

	return addressHeaders(addr), false
}

func cidHeader(cid *container.ID) eacl.Header {
	return &sysObjHdr{
		k: acl.FilterObjectContainerID,
		v: cidValue(cid),
	}
}

func oidHeader(oid *objectSDK.ID) eacl.Header {
	return &sysObjHdr{
		k: acl.FilterObjectID,
		v: idValue(oid),
	}
}

func addressHeaders(addr *objectSDK.Address) []eacl.Header {
	res := make([]eacl.Header, 1, 2)
	res[0] = cidHeader(addr.ContainerID())

	if oid := addr.ObjectID(); oid != nil {
		res = append(res, oidHeader(oid))
	}

	return res
}
