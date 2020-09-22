package searchsvc

import (
	"io"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	"github.com/pkg/errors"
)

type streamer struct {
	stream *searchsvc.Streamer
}

func (s *streamer) Recv() (*object.SearchResponse, error) {
	r, err := s.stream.Recv()
	if err != nil {
		if errors.Is(errors.Cause(err), io.EOF) {
			return nil, io.EOF
		}

		return nil, errors.Wrapf(err, "(%T) could not receive search response", s)
	}

	return fromResponse(r), nil
}
