package putsvc

import (
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

func (s *streamer) toInitPrm(part *objectV2.PutObjectPartInit, req *objectV2.PutRequest) (*putsvc.PutInitPrm, error) {
	oV2 := new(objectV2.Object)
	oV2.SetObjectID(part.GetObjectID())
	oV2.SetSignature(part.GetSignature())
	oV2.SetHeader(part.GetHeader())

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	return new(putsvc.PutInitPrm).
		WithObject(
			object.NewFromV2(oV2),
		).
		WithRelay(s.relayRequest).
		WithCommonPrm(commonPrm), nil
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
