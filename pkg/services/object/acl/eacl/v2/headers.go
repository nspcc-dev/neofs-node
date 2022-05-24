package v2

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	refsV2 "github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type Option func(*cfg)

type cfg struct {
	storage ObjectStorage

	msg xHeaderSource

	cid cidSDK.ID
	oid *oidSDK.ID
}

type ObjectStorage interface {
	Head(*addressSDK.Address) (*object.Object, error)
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

	if cfg.msg == nil {
		return nil, errors.New("message is not provided")
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
	return msg.GetXHeaders()
}

var errMissingOID = errors.New("object ID is missing")

func (h *cfg) objectHeaders() ([]eaclSDK.Header, error) {
	switch m := h.msg.(type) {
	default:
		panic(fmt.Sprintf("unexpected message type %T", h.msg))
	case requestXHeaderSource:
		switch req := m.req.(type) {
		case
			*objectV2.GetRequest,
			*objectV2.HeadRequest:
			if h.oid == nil {
				return nil, errMissingOID
			}

			return h.localObjectHeaders(h.cid, h.oid)
		case
			*objectV2.GetRangeRequest,
			*objectV2.GetRangeHashRequest,
			*objectV2.DeleteRequest:
			if h.oid == nil {
				return nil, errMissingOID
			}

			return addressHeaders(h.cid, h.oid), nil
		case *objectV2.PutRequest:
			if v, ok := req.GetBody().GetObjectPart().(*objectV2.PutObjectPartInit); ok {
				oV2 := new(objectV2.Object)
				oV2.SetObjectID(v.GetObjectID())
				oV2.SetHeader(v.GetHeader())

				return headersFromObject(object.NewFromV2(oV2), h.cid, h.oid), nil
			}
		case *objectV2.SearchRequest:
			cnrV2 := req.GetBody().GetContainerID()
			var cnr cidSDK.ID

			if cnrV2 != nil {
				if err := cnr.ReadFromV2(*cnrV2); err != nil {
					return nil, fmt.Errorf("can't parse container ID: %w", err)
				}
			}

			return []eaclSDK.Header{cidHeader(cnr)}, nil
		}
	case responseXHeaderSource:
		switch resp := m.resp.(type) {
		default:
			hs, _ := h.localObjectHeaders(h.cid, h.oid)
			return hs, nil
		case *objectV2.GetResponse:
			if v, ok := resp.GetBody().GetObjectPart().(*objectV2.GetObjectPartInit); ok {
				oV2 := new(objectV2.Object)
				oV2.SetObjectID(v.GetObjectID())
				oV2.SetHeader(v.GetHeader())

				return headersFromObject(object.NewFromV2(oV2), h.cid, h.oid), nil
			}
		case *objectV2.HeadResponse:
			oV2 := new(objectV2.Object)

			var hdr *objectV2.Header

			switch v := resp.GetBody().GetHeaderPart().(type) {
			case *objectV2.ShortHeader:
				hdr = new(objectV2.Header)

				var idV2 refsV2.ContainerID
				h.cid.WriteToV2(&idV2)

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

			return headersFromObject(object.NewFromV2(oV2), h.cid, h.oid), nil
		}
	}

	return nil, nil
}

func (h *cfg) localObjectHeaders(cid cidSDK.ID, oid *oidSDK.ID) ([]eaclSDK.Header, error) {
	var obj *objectSDK.Object
	var err error

	if oid != nil {
		var addr addressSDK.Address
		addr.SetContainerID(cid)
		addr.SetObjectID(*oid)

		obj, err = h.storage.Head(&addr)
		if err == nil {
			return headersFromObject(obj, cid, oid), nil
		}
	}

	// Still parse addressHeaders, because the errors is ignored in some places.
	return addressHeaders(cid, oid), err
}

func cidHeader(idCnr cidSDK.ID) sysObjHdr {
	return sysObjHdr{
		k: acl.FilterObjectContainerID,
		v: idCnr.EncodeToString(),
	}
}

func oidHeader(oid oidSDK.ID) sysObjHdr {
	return sysObjHdr{
		k: acl.FilterObjectID,
		v: oid.EncodeToString(),
	}
}

func ownerIDHeader(ownerID user.ID) sysObjHdr {
	return sysObjHdr{
		k: acl.FilterObjectOwnerID,
		v: ownerID.String(),
	}
}

func addressHeaders(cid cidSDK.ID, oid *oidSDK.ID) []eaclSDK.Header {
	hh := make([]eaclSDK.Header, 0, 2)
	hh = append(hh, cidHeader(cid))

	if oid != nil {
		hh = append(hh, oidHeader(*oid))
	}

	return hh
}
