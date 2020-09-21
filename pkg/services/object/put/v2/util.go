package putsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
)

func toInitPrm(req *objectV2.PutObjectPartInit, t *session.SessionToken, ttl uint32) *putsvc.PutInitPrm {
	oV2 := new(objectV2.Object)
	oV2.SetObjectID(req.GetObjectID())
	oV2.SetSignature(req.GetSignature())
	oV2.SetHeader(req.GetHeader())

	return new(putsvc.PutInitPrm).
		WithObject(
			object.NewRawFromV2(oV2),
		).
		WithSession(
			token.NewSessionTokenFromV2(t),
		).
		OnlyLocal(ttl == 1) // FIXME: use constant
}

func toChunkPrm(req *objectV2.PutObjectPartChunk) *putsvc.PutChunkPrm {
	return new(putsvc.PutChunkPrm).
		WithChunk(req.GetChunk())
}

func fromPutResponse(r *putsvc.PutResponse) *objectV2.PutResponse {
	body := new(objectV2.PutResponseBody)
	body.SetObjectID(r.ObjectID().ToV2())

	resp := new(objectV2.PutResponse)
	resp.SetBody(body)

	return resp
}
