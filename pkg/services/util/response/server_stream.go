package response

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/pkg/errors"
)

// ServerMessageStreamer represents server-side message streamer
// that sets meta values to all response messages.
type ServerMessageStreamer struct {
	cfg *cfg

	recv util.ResponseMessageReader
}

// Recv calls Recv method of internal streamer, sets response meta
// values and returns the response.
func (s *ServerMessageStreamer) Recv() (util.ResponseMessage, error) {
	m, err := s.recv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response message for signing")
	}

	setMeta(m, s.cfg)

	return m, nil
}

// HandleServerStreamRequest builds internal streamer via handlers, wraps it to ServerMessageStreamer and returns the result.
func (s *Service) HandleServerStreamRequest(ctx context.Context, req interface{}, handler util.ServerStreamHandler) (*ServerMessageStreamer, error) {
	msgRdr, err := handler(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "could not create message reader")
	}

	return &ServerMessageStreamer{
		cfg:  s.cfg,
		recv: msgRdr,
	}, nil
}
