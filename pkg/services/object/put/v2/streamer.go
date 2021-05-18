package putsvc

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
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
			err = fmt.Errorf("(%T) could not init object put stream: %w", s, err)
		}
	case *object.PutObjectPartChunk:
		if err = s.stream.SendChunk(toChunkPrm(v)); err != nil {
			err = fmt.Errorf("(%T) could not send payload chunk: %w", s, err)
		}
	default:
		err = fmt.Errorf("(%T) invalid object put stream part type %T", s, v)
	}

	return
}

func (s *streamer) CloseAndRecv() (*object.PutResponse, error) {
	resp, err := s.stream.Close()
	if err != nil {
		return nil, fmt.Errorf("(%T) could not object put stream: %w", s, err)
	}

	return fromPutResponse(resp), nil
}
