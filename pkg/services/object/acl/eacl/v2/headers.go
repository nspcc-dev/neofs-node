package v2

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	eaclSDK "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
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
			return h.localObjectHeaders(req.GetBody().GetAddress())
		case *objectV2.HeadRequest:
			return h.localObjectHeaders(req.GetBody().GetAddress())
		case *objectV2.GetRangeRequest:
			return h.localObjectHeaders(req.GetBody().GetAddress())
		case *objectV2.GetRangeHashRequest:
			return h.localObjectHeaders(req.GetBody().GetAddress())
		case *objectV2.PutRequest:
			if v, ok := req.GetBody().GetObjectPart().(*objectV2.PutObjectPartInit); ok {
				oV2 := new(objectV2.Object)
				oV2.SetObjectID(v.GetObjectID())
				oV2.SetHeader(v.GetHeader())

				return headersFromObject(object.NewFromV2(oV2)), true
			}
		}
	case *responseXHeaderSource:
		switch resp := m.resp.(type) {
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
			case *objectV2.GetHeaderPartShort:
				hdr = new(objectV2.Header)
				h := v.GetShortHeader()

				hdr.SetVersion(h.GetVersion())
				hdr.SetCreationEpoch(h.GetCreationEpoch())
				hdr.SetOwnerID(h.GetOwnerID())
				hdr.SetObjectType(h.GetObjectType())
				hdr.SetPayloadLength(h.GetPayloadLength())
			case *objectV2.GetHeaderPartFull:
				hdr = v.GetHeaderWithSignature().GetHeader()
			}

			oV2.SetHeader(hdr)

			return headersFromObject(object.NewFromV2(oV2)), true
		}
	}

	return nil, true
}

func (h *headerSource) localObjectHeaders(addr *refs.Address) ([]eacl.Header, bool) {
	obj, err := h.storage.Head(objectSDK.NewAddressFromV2(addr))
	if err == nil {
		return headersFromObject(obj), true
	}

	return nil, false
}
