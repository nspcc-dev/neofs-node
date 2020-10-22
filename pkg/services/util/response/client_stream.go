package response

import (
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/pkg/errors"
)

// ClientMessageStreamer represents client-side message streamer
// that sets meta values to the response.
type ClientMessageStreamer struct {
	cfg *cfg

	send util.RequestMessageWriter

	close util.ClientStreamCloser
}

// Recv calls send method of internal streamer.
func (s *ClientMessageStreamer) Send(req interface{}) error {
	return errors.Wrapf(
		s.send(req),
		"(%T) could not send the request", s)
}

// CloseAndRecv closes internal stream, receivers the response,
// sets meta values and returns the result.
func (s *ClientMessageStreamer) CloseAndRecv() (util.ResponseMessage, error) {
	resp, err := s.close()
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not close stream and receive response", s)
	}

	setMeta(resp, s.cfg)

	return resp, nil
}

// CreateRequestStreamer wraps stream methods and returns ClientMessageStreamer instance.
func (s *Service) CreateRequestStreamer(sender util.RequestMessageWriter, closer util.ClientStreamCloser) *ClientMessageStreamer {
	return &ClientMessageStreamer{
		cfg:   s.cfg,
		send:  sender,
		close: closer,
	}
}
