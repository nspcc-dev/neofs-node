package v2

import (
	"errors"
	"fmt"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	protoobject "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	refsV2 "github.com/nspcc-dev/neofs-api-go/v2/refs"
	refs "github.com/nspcc-dev/neofs-api-go/v2/refs/grpc"
	session "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type Option func(*cfg)

type cfg struct {
	storage      ObjectStorage
	headerSource HeaderSource

	msg xHeaderSource

	cnr cid.ID
	obj *oid.ID
}

type ObjectStorage interface {
	Head(oid.Address) (*object.Object, error)
}

// HeaderSource represents a source of the object headers.
type HeaderSource interface {
	// Head returns object (may be with or be without payload) by its address.
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

type xHeader [2]string

func (x xHeader) Key() string {
	return x[0]
}

func (x xHeader) Value() string {
	return x[1]
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
			*protoobject.GetRequest,
			*protoobject.HeadRequest:
			if h.obj == nil {
				return errMissingOID
			}

			objHeaders, completed := h.localObjectHeaders(h.cnr, h.obj)

			dst.objectHeaders = objHeaders
			dst.incompleteObjectHeaders = !completed
		case
			*protoobject.GetRangeRequest,
			*protoobject.GetRangeHashRequest,
			*protoobject.DeleteRequest:
			if h.obj == nil {
				return errMissingOID
			}

			dst.objectHeaders = addressHeaders(h.cnr, h.obj)
		case *protoobject.PutRequest:
			if v, ok := req.GetBody().GetObjectPart().(*protoobject.PutRequest_Body_Init_); ok {
				if v == nil || v.Init == nil {
					return errors.New("nil oneof field with heading part")
				}
				in := v.Init
				splitHeader := in.Header.GetSplit()
				if splitHeader == nil || splitHeader.SplitId != nil {
					// V1 split scheme or small object, only the received
					// object's header can be checked
					mo := &protoobject.Object{
						ObjectId: in.ObjectId,
						Header:   in.Header,
					}
					oV2 := new(objectV2.Object)
					if err := oV2.FromGRPCMessage(mo); err != nil {
						panic(err)
					}

					var obj object.Object
					err := obj.ReadFromV2(*oV2)
					if err != nil {
						return err
					}
					dst.objectHeaders = headersFromObject(&obj, h.cnr, h.obj)

					break
				}

				// V2 split case

				parentHeader := splitHeader.GetParentHeader()
				if parentHeader != nil {
					mo := &protoobject.Object{
						ObjectId:  splitHeader.Parent,
						Signature: splitHeader.ParentSignature,
						Header:    parentHeader,
					}
					var parentObjectV2 objectV2.Object
					if err := parentObjectV2.FromGRPCMessage(mo); err != nil {
						panic(err)
					}

					var obj object.Object
					err := obj.ReadFromV2(parentObjectV2)
					if err != nil {
						return err
					}
					dst.objectHeaders = headersFromObject(&obj, h.cnr, h.obj)
				} else {
					// middle object, parent header should
					// be received via the first object
					if mf := in.Header.GetSplit().GetFirst(); mf != nil {
						var firstID oid.ID
						var first refsV2.ObjectID
						if err := first.FromGRPCMessage(mf); err != nil {
							panic(err)
						}
						err := firstID.ReadFromV2(first)
						if err != nil {
							return fmt.Errorf("converting first object ID: %w", err)
						}

						var addr oid.Address
						addr.SetObject(firstID)
						addr.SetContainer(h.cnr)

						firstObject, err := h.headerSource.Head(addr)
						if err != nil {
							return fmt.Errorf("fetching first object header: %w", err)
						}

						dst.objectHeaders = headersFromObject(firstObject.Parent(), h.cnr, h.obj)
					}

					// first object not defined, unexpected, do not attach any header
				}
			}
		case *protoobject.SearchRequest:
			var cnr cid.ID

			if mc := req.GetBody().GetContainerId(); mc != nil {
				var cnrV2 refsV2.ContainerID
				if err := cnrV2.FromGRPCMessage(mc); err != nil {
					panic(err)
				}
				if err := cnr.ReadFromV2(cnrV2); err != nil {
					return fmt.Errorf("can't parse container ID: %w", err)
				}
			}

			dst.objectHeaders = []eaclSDK.Header{cidHeader(cnr)}
		}
	case responseXHeaderSource:
		switch resp := m.resp.(type) {
		default:
			objectHeaders, completed := h.localObjectHeaders(h.cnr, h.obj)

			dst.objectHeaders = objectHeaders
			dst.incompleteObjectHeaders = !completed
		case *protoobject.GetResponse:
			if v, ok := resp.GetBody().GetObjectPart().(*protoobject.GetResponse_Body_Init_); ok {
				if v == nil || v.Init == nil {
					return errors.New("nil oneof field with heading part")
				}
				mo := &protoobject.Object{
					ObjectId: v.Init.ObjectId,
					Header:   v.Init.Header,
				}
				oV2 := new(objectV2.Object)
				if err := oV2.FromGRPCMessage(mo); err != nil {
					panic(err)
				}

				var obj object.Object
				err := obj.ReadFromV2(*oV2)
				if err != nil {
					return err
				}
				dst.objectHeaders = headersFromObject(&obj, h.cnr, h.obj)
			}
		case *protoobject.HeadResponse:
			var hdr *protoobject.Header

			switch v := resp.GetBody().GetHead().(type) {
			case *protoobject.HeadResponse_Body_ShortHeader:
				if v == nil || v.ShortHeader == nil {
					return errors.New("nil oneof field with short header")
				}

				var idV2 refsV2.ContainerID
				h.cnr.WriteToV2(&idV2)

				h := v.ShortHeader
				hdr = &protoobject.Header{
					Version:       h.Version,
					ContainerId:   idV2.ToGRPCMessage().(*refs.ContainerID),
					OwnerId:       h.OwnerId,
					CreationEpoch: h.CreationEpoch,
					PayloadLength: h.PayloadLength,
					ObjectType:    h.ObjectType,
				}
			case *protoobject.HeadResponse_Body_Header:
				if v == nil || v.Header == nil {
					return errors.New("nil oneof field carrying header with signature")
				}
				hdr = v.Header.Header
			}

			mo := &protoobject.Object{
				Header: hdr,
			}
			oV2 := new(objectV2.Object)
			if err := oV2.FromGRPCMessage(mo); err != nil {
				panic(err)
			}

			var obj object.Object
			err := obj.ReadFromV2(*oV2)
			if err != nil {
				return err
			}
			dst.objectHeaders = headersFromObject(&obj, h.cnr, h.obj)
		}
	}

	return nil
}

func (h *cfg) localObjectHeaders(cnr cid.ID, idObj *oid.ID) ([]eaclSDK.Header, bool) {
	if idObj != nil {
		var addr oid.Address
		addr.SetContainer(cnr)
		addr.SetObject(*idObj)

		obj, err := h.storage.Head(addr)
		if err == nil {
			return headersFromObject(obj, cnr, idObj), true
		}
	}

	return addressHeaders(cnr, idObj), false
}

func cidHeader(idCnr cid.ID) sysObjHdr {
	return sysObjHdr{
		k: eaclSDK.FilterObjectContainerID,
		v: idCnr.EncodeToString(),
	}
}

func oidHeader(obj oid.ID) sysObjHdr {
	return sysObjHdr{
		k: eaclSDK.FilterObjectID,
		v: obj.EncodeToString(),
	}
}

func ownerIDHeader(ownerID user.ID) sysObjHdr {
	return sysObjHdr{
		k: eaclSDK.FilterObjectOwnerID,
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
