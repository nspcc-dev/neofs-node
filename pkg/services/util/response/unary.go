package response

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

// HandleUnaryRequest call passes request to handler, sets response meta header values and returns it.
func (s *Service) HandleUnaryRequest(ctx context.Context, req interface{}, handler util.UnaryHandler) (util.ResponseMessage, error) {
	// process request
	resp, err := handler(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("could not handle request: %w", err)
	}

	setMeta(resp, s.cfg)

	return resp, nil
}
