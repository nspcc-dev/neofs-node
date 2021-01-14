package putsvc

import (
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"github.com/pkg/errors"
)

type streamer struct {
	stream *putsvc.Streamer
}

func (s *streamer) Send(req *object.PutRequest) (err error) {
	switch v := req.GetBody().GetObjectPart().(type) {
	case *object.PutObjectPartInit:
		var initPrm *putsvc.PutInitPrm

		initPrm, err = toInitPrm(v, req)
		if err != nil {
			return err
		}

		if err = s.stream.Init(initPrm); err != nil {
			err = errors.Wrapf(err, "(%T) could not init object put stream", s)
		}
	case *object.PutObjectPartChunk:
		if err = s.stream.SendChunk(toChunkPrm(v)); err != nil {
			err = errors.Wrapf(err, "(%T) could not send payload chunk", s)
		}
	default:
		err = errors.Errorf("(%T) invalid object put stream part type %T", s, v)
	}

	return
}

func (s *streamer) CloseAndRecv() (*object.PutResponse, error) {
	resp, err := s.stream.Close()
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not object put stream", s)
	}

	return fromPutResponse(resp), nil
}
