package session

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	protosession "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
)

// Server is an interface of the NeoFS API Session service server.
type Server interface {
	Create(context.Context, *session.CreateRequest) (*session.CreateResponse, error)
}

type server struct {
	protosession.UnimplementedSessionServiceServer
	srv Server
}

// New returns protosession.SessionServiceServer based on the Server.
func New(c Server) protosession.SessionServiceServer {
	return &server{
		srv: c,
	}
}

// Create converts gRPC CreateRequest message and passes it to internal Session service.
func (s *server) Create(ctx context.Context, req *protosession.CreateRequest) (*protosession.CreateResponse, error) {
	createReq := new(session.CreateRequest)
	if err := createReq.FromGRPCMessage(req); err != nil {
		return nil, err
	}

	resp, err := s.srv.Create(ctx, createReq)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protosession.CreateResponse), nil
}
