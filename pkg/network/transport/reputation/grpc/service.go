package grpcreputation

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/reputation"
	reputation2 "github.com/nspcc-dev/neofs-api-go/v2/reputation/grpc"
	reputationrpc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/rpc"
)

// Server wraps NeoFS API v2 Reputation service server
// and provides gRPC Reputation service server interface.
type Server struct {
	srv reputationrpc.Server
}

// New creates, initializes and returns Server instance.
func New(srv reputationrpc.Server) *Server {
	return &Server{
		srv: srv,
	}
}

func (s *Server) SendLocalTrust(ctx context.Context, r *reputation2.SendLocalTrustRequest) (*reputation2.SendLocalTrustResponse, error) {
	req := new(reputation.SendLocalTrustRequest)
	if err := req.FromGRPCMessage(r); err != nil {
		return nil, err
	}

	resp, err := s.srv.SendLocalTrust(ctx, req)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*reputation2.SendLocalTrustResponse), nil
}

func (s *Server) SendIntermediateResult(ctx context.Context, r *reputation2.SendIntermediateResultRequest) (*reputation2.SendIntermediateResultResponse, error) {
	req := new(reputation.SendIntermediateResultRequest)
	if err := req.FromGRPCMessage(r); err != nil {
		return nil, err
	}

	resp, err := s.srv.SendIntermediateResult(ctx, req)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*reputation2.SendIntermediateResultResponse), nil
}
