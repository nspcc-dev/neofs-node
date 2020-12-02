package getsvc

import (
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
)

type streamObjectWriter struct {
	objectSvc.GetObjectStream
}

func (s *streamObjectWriter) WriteHeader(obj *object.Object) error {
	p := new(objectV2.GetObjectPartInit)

	objV2 := obj.ToV2()
	p.SetObjectID(objV2.GetObjectID())
	p.SetHeader(objV2.GetHeader())
	p.SetSignature(objV2.GetSignature())

	return s.GetObjectStream.Send(newResponse(p))
}

func (s *streamObjectWriter) WriteChunk(chunk []byte) error {
	p := new(objectV2.GetObjectPartChunk)
	p.SetChunk(chunk)

	return s.GetObjectStream.Send(newResponse(p))
}

func newResponse(p objectV2.GetObjectPart) *objectV2.GetResponse {
	r := new(objectV2.GetResponse)

	body := new(objectV2.GetResponseBody)
	r.SetBody(body)

	body.SetObjectPart(p)

	return r
}
