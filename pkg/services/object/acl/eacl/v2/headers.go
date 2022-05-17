package v2

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	refsV2 "github.com/nspcc-dev/neofs-api-go/v2/refs"
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
	requestHeaders []eaclSDK.Header
	objectHeaders  []eaclSDK.Header
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

	objHdrs, err := cfg.objectHeaders()
	if err != nil {
		return nil, err
	}

	return headerSource{
		objectHeaders:  objHdrs,
		requestHeaders: requestHeaders(cfg.msg),
	}, nil
}

func (h headerSource) HeadersOfType(typ eaclSDK.FilterHeaderType) ([]eaclSDK.Header, bool) {
	switch typ {
	default:
		return nil, true
	case eaclSDK.HeaderFromRequest:
		return h.requestHeaders, true
	case eaclSDK.HeaderFromObject:
		return h.objectHeaders, true
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

func (h *cfg) objectHeaders() ([]eaclSDK.Header, error) {
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
			return addressHeaders(h.addr), nil
		case *objectV2.PutRequest:
			if v, ok := req.GetBody().GetObjectPart().(*objectV2.PutObjectPartInit); ok {
				oV2 := new(objectV2.Object)
				oV2.SetObjectID(v.GetObjectID())
				oV2.SetHeader(v.GetHeader())

				if h.addr == nil {
					idV2 := v.GetObjectID()
					var id objectSDKID.ID

					if idV2 != nil {
						if err := id.ReadFromV2(*idV2); err != nil {
							return nil, fmt.Errorf("can't parse object ID: %w", err)
						}
					}

					cnrV2 := v.GetHeader().GetContainerID()
					var cnr cid.ID

					if cnrV2 != nil {
						if err := cnr.ReadFromV2(*cnrV2); err != nil {
							return nil, fmt.Errorf("can't parse container ID: %w", err)
						}
					}

					h.addr = new(objectSDKAddress.Address)
					h.addr.SetContainerID(cnr)
					h.addr.SetObjectID(id)
				}

				hs := headersFromObject(object.NewFromV2(oV2), h.addr)

				return hs, nil
			}
		case *objectV2.SearchRequest:
			cnrV2 := req.GetBody().GetContainerID()
			var cnr cid.ID

			if cnrV2 != nil {
				if err := cnr.ReadFromV2(*cnrV2); err != nil {
					return nil, fmt.Errorf("can't parse container ID: %w", err)
				}
			}

			return []eaclSDK.Header{cidHeader(&cnr)}, nil
		}
	case *responseXHeaderSource:
		switch resp := m.resp.(type) {
		default:
			hs, _ := h.localObjectHeaders(h.addr)
			return hs, nil
		case *objectV2.GetResponse:
			if v, ok := resp.GetBody().GetObjectPart().(*objectV2.GetObjectPartInit); ok {
				oV2 := new(objectV2.Object)
				oV2.SetObjectID(v.GetObjectID())
				oV2.SetHeader(v.GetHeader())

				return headersFromObject(object.NewFromV2(oV2), h.addr), nil
			}
		case *objectV2.HeadResponse:
			oV2 := new(objectV2.Object)

			var hdr *objectV2.Header

			switch v := resp.GetBody().GetHeaderPart().(type) {
			case *objectV2.ShortHeader:
				hdr = new(objectV2.Header)

				id, _ := h.addr.ContainerID()

				var idV2 refsV2.ContainerID
				id.WriteToV2(&idV2)

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

			return headersFromObject(object.NewFromV2(oV2), h.addr), nil
		}
	}

	return nil, nil
}

func (h *cfg) localObjectHeaders(addr *objectSDKAddress.Address) ([]eaclSDK.Header, error) {
	obj, err := h.storage.Head(addr)
	if err != nil {
		// Still parse addressHeaders, because the errors is ignored in some places.
		return addressHeaders(addr), err
	}
	return headersFromObject(obj, addr), nil
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
	cnr, _ := addr.ContainerID()

	res := make([]eaclSDK.Header, 1, 2)
	res[0] = cidHeader(&cnr)

	if oid, ok := addr.ObjectID(); ok {
		res = append(res, oidHeader(&oid))
	}

	return res
}
