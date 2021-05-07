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

func (s *Server) AnnounceLocalTrust(ctx context.Context, r *reputation2.AnnounceLocalTrustRequest) (*reputation2.AnnounceLocalTrustResponse, error) {
	req := new(reputation.AnnounceLocalTrustRequest)
	if err := req.FromGRPCMessage(r); err != nil {
		return nil, err
	}

	resp, err := s.srv.AnnounceLocalTrust(ctx, req)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*reputation2.AnnounceLocalTrustResponse), nil
}

func (s *Server) AnnounceIntermediateResult(ctx context.Context, r *reputation2.AnnounceIntermediateResultRequest) (*reputation2.AnnounceIntermediateResultResponse, error) {
	req := new(reputation.AnnounceIntermediateResultRequest)
	if err := req.FromGRPCMessage(r); err != nil {
		return nil, err
	}

	resp, err := s.srv.AnnounceIntermediateResult(ctx, req)
	if err != nil {
		// TODO: think about how we transport errors through gRPC
		return nil, err
	}

	return resp.ToGRPCMessage().(*reputation2.AnnounceIntermediateResultResponse), nil
}
