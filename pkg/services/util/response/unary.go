package response

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/pkg/errors"
)

// HandleUnaryRequest call passes request to handler, sets response meta header values and returns it.
func (s *Service) HandleUnaryRequest(ctx context.Context, req interface{}, handler util.UnaryHandler) (util.ResponseMessage, error) {
	// process request
	resp, err := handler(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "could not handle request")
	}

	setMeta(resp, s.cfg)

	return resp, nil
}
