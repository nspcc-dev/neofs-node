package reputationrpc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/reputation"
	protoreputation "github.com/nspcc-dev/neofs-api-go/v2/reputation/grpc"
)

// Server is an interface of the NeoFS API v2 Reputation service server.
type Server interface {
	AnnounceLocalTrust(context.Context, *reputation.AnnounceLocalTrustRequest) (*reputation.AnnounceLocalTrustResponse, error)
	AnnounceIntermediateResult(context.Context, *reputation.AnnounceIntermediateResultRequest) (*reputation.AnnounceIntermediateResultResponse, error)
}

type server struct {
	protoreputation.ReputationServiceServer
	srv Server
}

// New returns protoreputation.ReputationServiceServer based on the Server.
func New(srv Server) protoreputation.ReputationServiceServer {
	return &server{
		srv: srv,
	}
}

func (s *server) AnnounceLocalTrust(ctx context.Context, r *protoreputation.AnnounceLocalTrustRequest) (*protoreputation.AnnounceLocalTrustResponse, error) {
	req := new(reputation.AnnounceLocalTrustRequest)
	if err := req.FromGRPCMessage(r); err != nil {
		return nil, err
	}

	resp, err := s.srv.AnnounceLocalTrust(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protoreputation.AnnounceLocalTrustResponse), nil
}

func (s *server) AnnounceIntermediateResult(ctx context.Context, r *protoreputation.AnnounceIntermediateResultRequest) (*protoreputation.AnnounceIntermediateResultResponse, error) {
	req := new(reputation.AnnounceIntermediateResultRequest)
	if err := req.FromGRPCMessage(r); err != nil {
		return nil, err
	}

	resp, err := s.srv.AnnounceIntermediateResult(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.ToGRPCMessage().(*protoreputation.AnnounceIntermediateResultResponse), nil
}
