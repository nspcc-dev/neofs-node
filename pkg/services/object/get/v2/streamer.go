package getsvc

import (
	"fmt"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/pkg/errors"
)

type streamer struct {
	stream *getsvc.Streamer

	body *objectV2.GetResponseBody
}

func (s *streamer) Recv() (*objectV2.GetResponse, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not receive get response", s)
	}

	switch v := r.(type) {
	case *object.Object:
		oV2 := v.ToV2()

		partInit := new(objectV2.GetObjectPartInit)
		partInit.SetHeader(oV2.GetHeader())
		partInit.SetSignature(oV2.GetSignature())
		partInit.SetObjectID(oV2.GetObjectID())

		s.body.SetObjectPart(partInit)
	case []byte:
		partChunk := new(objectV2.GetObjectPartChunk)
		partChunk.SetChunk(v)

		s.body.SetObjectPart(partChunk)
	default:
		panic(fmt.Sprintf("unexpected response type %T from %T", r, s.stream))
	}

	resp := new(objectV2.GetResponse)
	resp.SetBody(s.body)

	return resp, nil
}
