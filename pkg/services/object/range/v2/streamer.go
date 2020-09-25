package rangesvc

import (
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	rangesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/range"
	"github.com/pkg/errors"
)

type streamer struct {
	stream rangesvc.Streamer

	body *objectV2.GetRangeResponseBody
}

func (s *streamer) Recv() (*objectV2.GetRangeResponse, error) {
	r, err := s.stream.Recv()
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not read response from stream", s)
	}

	s.body.SetChunk(r.PayloadChunk())

	resp := new(objectV2.GetRangeResponse)
	resp.SetBody(s.body)

	return resp, nil
}
