package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	sessionGRPC "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
)

// Server wraps NeoFS API Session service and
// provides gRPC Session service server interface.
type Server struct {
	srv session.Service
}

// New creates, initializes and returns Server instance.
func New(c session.Service) *Server {
	return &Server{
		srv: c,
	}
}

// Create converts gRPC CreateRequest message and passes it to internal Session service.
func (s *Server) Create(ctx context.Context, req *sessionGRPC.CreateRequest) (*sessionGRPC.CreateResponse, error) {
	resp, err := s.srv.Create(ctx, session.CreateRequestFromGRPCMessage(req))
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return session.CreateResponseToGRPCMessage(resp), nil
}
