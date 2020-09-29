package putsvc

import (
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

func toInitPrm(part *objectV2.PutObjectPartInit, req *objectV2.PutRequest) *putsvc.PutInitPrm {
	oV2 := new(objectV2.Object)
	oV2.SetObjectID(part.GetObjectID())
	oV2.SetSignature(part.GetSignature())
	oV2.SetHeader(part.GetHeader())

	return new(putsvc.PutInitPrm).
		WithObject(
			object.NewRawFromV2(oV2),
		).
		WithCommonPrm(util.CommonPrmFromV2(req))
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
