package acl

import (
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/acl"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
)

type objectHeaderSource struct {
	obj *object.Object
}

type typedHeader struct {
	n string
	v string
	t acl.HeaderType
}

type extendedHeadersWrapper struct {
	hdrSrc service.ExtendedHeadersSource
}

type typedExtendedHeader struct {
	hdr service.ExtendedHeader
}

func newTypedObjSysHdr(name, value string) acl.TypedHeader {
	return &typedHeader{
		n: name,
		v: value,
		t: acl.HdrTypeObjSys,
	}
}

// Name is a name field getter.
func (s typedHeader) Name() string {
	return s.n
}

// Value is a value field getter.
func (s typedHeader) Value() string {
	return s.v
}

// HeaderType is a type field getter.
func (s typedHeader) HeaderType() acl.HeaderType {
	return s.t
}

// TypedHeaderSourceFromObject wraps passed object and returns TypedHeaderSource interface.
func TypedHeaderSourceFromObject(obj *object.Object) TypedHeaderSource {
	return &objectHeaderSource{
		obj: obj,
	}
}

// HeaderOfType gathers object headers of passed type and returns Header list.
//
// If value of some header can not be calculated (e.g. nil extended header), it does not appear in list.
//
// Always returns true.
func (s objectHeaderSource) HeadersOfType(typ acl.HeaderType) ([]acl.Header, bool) {
	if s.obj == nil {
		return nil, true
	}

	var res []acl.Header

	switch typ {
	case acl.HdrTypeObjUsr:
		objHeaders := s.obj.GetHeaders()

		res = make([]acl.Header, 0, len(objHeaders)) // 7 system header fields

		for i := range objHeaders {
			if h := newTypedObjectExtendedHeader(objHeaders[i]); h != nil {
				res = append(res, h)
			}
		}
	case acl.HdrTypeObjSys:
		res = make([]acl.Header, 0, 7)

		sysHdr := s.obj.GetSystemHeader()

		created := sysHdr.GetCreatedAt()

		res = append(res,
			// ID
			newTypedObjSysHdr(
				acl.HdrObjSysNameID,
				sysHdr.ID.String(),
			),

			// CID
			newTypedObjSysHdr(
				acl.HdrObjSysNameCID,
				sysHdr.CID.String(),
			),

			// OwnerID
			newTypedObjSysHdr(
				acl.HdrObjSysNameOwnerID,
				sysHdr.OwnerID.String(),
			),

			// Version
			newTypedObjSysHdr(
				acl.HdrObjSysNameVersion,
				strconv.FormatUint(sysHdr.GetVersion(), 10),
			),

			// PayloadLength
			newTypedObjSysHdr(
				acl.HdrObjSysNamePayloadLength,
				strconv.FormatUint(sysHdr.GetPayloadLength(), 10),
			),

			// CreatedAt.UnitTime
			newTypedObjSysHdr(
				acl.HdrObjSysNameCreatedUnix,
				strconv.FormatUint(uint64(created.GetUnixTime()), 10),
			),

			// CreatedAt.Epoch
			newTypedObjSysHdr(
				acl.HdrObjSysNameCreatedEpoch,
				strconv.FormatUint(created.GetEpoch(), 10),
			),
		)
	}

	return res, true
}

func newTypedObjectExtendedHeader(h object.Header) acl.TypedHeader {
	val := h.GetValue()
	if val == nil {
		return nil
	}

	res := new(typedHeader)
	res.t = acl.HdrTypeObjSys

	switch hdr := val.(type) {
	case *object.Header_UserHeader:
		if hdr.UserHeader == nil {
			return nil
		}

		res.t = acl.HdrTypeObjUsr
		res.n = hdr.UserHeader.GetKey()
		res.v = hdr.UserHeader.GetValue()
	case *object.Header_Link:
		if hdr.Link == nil {
			return nil
		}

		switch hdr.Link.GetType() {
		case object.Link_Previous:
			res.n = acl.HdrObjSysLinkPrev
		case object.Link_Next:
			res.n = acl.HdrObjSysLinkNext
		case object.Link_Child:
			res.n = acl.HdrObjSysLinkChild
		case object.Link_Parent:
			res.n = acl.HdrObjSysLinkPar
		case object.Link_StorageGroup:
			res.n = acl.HdrObjSysLinkSG
		default:
			return nil
		}

		res.v = hdr.Link.ID.String()
	default:
		return nil
	}

	return res
}

// TypedHeaderSourceFromExtendedHeaders wraps passed ExtendedHeadersSource and returns TypedHeaderSource interface.
func TypedHeaderSourceFromExtendedHeaders(hdrSrc service.ExtendedHeadersSource) TypedHeaderSource {
	return &extendedHeadersWrapper{
		hdrSrc: hdrSrc,
	}
}

// Name returns the result of Key method.
func (s typedExtendedHeader) Name() string {
	return s.hdr.Key()
}

// Value returns the result of Value method.
func (s typedExtendedHeader) Value() string {
	return s.hdr.Value()
}

// HeaderType always returns HdrTypeRequest.
func (s typedExtendedHeader) HeaderType() acl.HeaderType {
	return acl.HdrTypeRequest
}

// TypedHeaders gathers extended request headers and returns TypedHeader list.
//
// Nil headers are ignored.
//
// Always returns true.
func (s extendedHeadersWrapper) HeadersOfType(typ acl.HeaderType) ([]acl.Header, bool) {
	if s.hdrSrc == nil {
		return nil, true
	}

	var res []acl.Header

	if typ == acl.HdrTypeRequest {
		hs := s.hdrSrc.ExtendedHeaders()

		res = make([]acl.Header, 0, len(hs))

		for i := range hs {
			if hs[i] == nil {
				continue
			}

			res = append(res, &typedExtendedHeader{
				hdr: hs[i],
			})
		}
	}

	return res, true
}
