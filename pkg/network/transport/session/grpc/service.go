package session

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	sessionGRPC "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	sessionsvc "github.com/nspcc-dev/neofs-node/pkg/services/session"
)

// Server wraps NeoFS API Session service and
// provides gRPC Session service server interface.
type Server struct {
	srv sessionsvc.Server
}

// New creates, initializes and returns Server instance.
func New(c sessionsvc.Server) *Server {
	return &Server{
		srv: c,
	}
}

// Create converts gRPC CreateRequest message and passes it to internal Session service.
func (s *Server) Create(ctx context.Context, req *sessionGRPC.CreateRequest) (*sessionGRPC.CreateResponse, error) {
	createReq := new(session.CreateRequest)
	if err := createReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Create(ctx, createReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*sessionGRPC.CreateResponse), nil
}
