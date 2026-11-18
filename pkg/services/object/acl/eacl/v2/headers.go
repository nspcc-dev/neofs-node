package v2

import (
	"context"
	"errors"
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/session"
)

type Option func(*cfg)

type binaryHeader []byte

type cfg struct {
	ctx          context.Context
	storage      ObjectStorage
	headerSource HeaderSource

	msg any

	cnr cid.ID
	obj oid.ID
}

type ObjectStorage interface {
	Head(ctx context.Context, addr oid.Address) (*object.Object, error)
}

// HeaderSource represents a source of the object headers.
type HeaderSource interface {
	// Head returns object (may be with or be without payload) by its address.
	Head(ctx context.Context, addr oid.Address) (*object.Object, error)
}

type Request interface {
	GetMetaHeader() *session.RequestMetaHeader
}

type Response interface {
	GetMetaHeader() *session.ResponseMetaHeader
}

type headerSource struct {
	cfg cfg

	obj         *object.Object
	loaded      bool
	binaryValid bool
	addressOnly bool
	loadErr     error
	scratch     []byte
}

func defaultCfg() *cfg {
	return &cfg{
		ctx:     context.Background(),
		storage: new(localStorage),
	}
}

func NewMessageHeaderSource(opts ...Option) (eacl.TypedHeaderSource, error) {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	if cfg.msg == nil {
		return nil, errors.New("message is not provided")
	}

	var res = &headerSource{
		cfg: *cfg,
	}

	return res, nil
}

func (h *headerSource) HeaderValue(typ eacl.FilterHeaderType, key string) ([]byte, error) {
	var value string
	var found bool
	switch typ {
	case eacl.HeaderTypeUnspecified, eacl.HeaderFromService:
		return nil, nil
	case eacl.HeaderFromRequest:
		switch msg := h.cfg.msg.(type) {
		case requestXHeaderSource:
			value, found = msg.headerValue(key)
		case responseXHeaderSource:
			value, found = msg.req.headerValue(key)
		}
	case eacl.HeaderFromObject:
		if msg, ok := h.cfg.msg.(binaryHeader); ok {
			value, err := lookupBinaryObjectHeader(msg, h.cfg.cnr, h.cfg.obj, key, !h.binaryValid)
			h.binaryValid = err == nil
			return value, err
		}
		if !h.loaded {
			h.loadErr = h.cfg.readObjectHeader(h)
			h.loaded = true
		}
		if h.loadErr != nil {
			return nil, h.loadErr
		}
		value, found = lookupObjectHeader(h.obj, h.cfg.cnr, h.cfg.obj, h.addressOnly, key)
	}
	if !found {
		return nil, nil
	}
	if value == "" {
		return []byte{}, nil
	}
	h.scratch = append(h.scratch[:0], value...)
	return h.scratch, nil
}

func (h *cfg) readObjectHeader(dst *headerSource) error {
	switch m := h.msg.(type) {
	default:
		panic(fmt.Sprintf("unexpected message type %T", h.msg))
	case requestXHeaderSource:
		switch req := m.req.(type) {
		case
			*protoobject.GetRequest,
			*protoobject.HeadRequest:
			var err error
			dst.obj, err = h.localObject()
			return err
		case
			*protoobject.DeleteRequest,
			*protoobject.SearchV2Request:
			dst.addressOnly = true
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

					var obj object.Object
					err := obj.FromProtoMessage(mo)
					if err != nil {
						return err
					}
					dst.obj = &obj

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

					var obj object.Object
					err := obj.FromProtoMessage(mo)
					if err != nil {
						return err
					}
					dst.obj = &obj
				} else {
					// middle object, parent header should
					// be received via the first object
					if mf := in.Header.GetSplit().GetFirst(); mf != nil {
						var firstID oid.ID

						err := firstID.FromProtoMessage(mf)
						if err != nil {
							return fmt.Errorf("converting first object ID: %w", err)
						}

						var addr oid.Address
						addr.SetObject(firstID)
						addr.SetContainer(h.cnr)

						firstObject, err := h.headerSource.Head(h.ctx, addr)
						if err != nil {
							return fmt.Errorf("fetching first object header: %w", err)
						}

						dst.obj = firstObject.Parent()
					}

					// first object not defined, unexpected, do not attach any header
				}
			}
		}
	case responseXHeaderSource:
		switch resp := m.resp.(type) {
		default:
			var err error
			dst.obj, err = h.localObject()
			return err
		case *protoobject.GetResponse:
			if v, ok := resp.GetBody().GetObjectPart().(*protoobject.GetResponse_Body_Init_); ok {
				if v == nil || v.Init == nil {
					return errors.New("nil oneof field with heading part")
				}
				mo := &protoobject.Object{
					ObjectId: v.Init.ObjectId,
					Header:   v.Init.Header,
				}

				var obj object.Object
				err := obj.FromProtoMessage(mo)
				if err != nil {
					return err
				}
				dst.obj = &obj
			}
		case *protoobject.HeadResponse:
			var hdr *protoobject.Header

			switch v := resp.GetBody().GetHead().(type) {
			case *protoobject.HeadResponse_Body_ShortHeader:
				if v == nil || v.ShortHeader == nil {
					return errors.New("nil oneof field with short header")
				}

				idMsg := h.cnr.ProtoMessage()

				h := v.ShortHeader
				hdr = &protoobject.Header{
					Version:       h.Version,
					ContainerId:   idMsg,
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

			var obj object.Object
			err := obj.FromProtoMessage(mo)
			if err != nil {
				return err
			}
			dst.obj = &obj
		}
	}

	return nil
}

func (h *cfg) localObject() (*object.Object, error) {
	if !h.obj.IsZero() {
		obj, err := h.storage.Head(h.ctx, oid.NewAddress(h.cnr, h.obj))
		if err == nil {
			return obj, nil
		}
	}
	return nil, eacl.ErrHeaderUnavailable
}
