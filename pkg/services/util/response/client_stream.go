package response

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/util"
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
	if err := s.send(req); err != nil {
		return fmt.Errorf("(%T) could not send the request: %w", s, err)
	}
	return nil
}

// CloseAndRecv closes internal stream, receivers the response,
// sets meta values and returns the result.
func (s *ClientMessageStreamer) CloseAndRecv() (util.ResponseMessage, error) {
	resp, err := s.close()
	if err != nil {
		return nil, fmt.Errorf("(%T) could not close stream and receive response: %w", s, err)
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
