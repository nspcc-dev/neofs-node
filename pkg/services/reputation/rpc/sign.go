package reputationrpc

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

type signService struct {
	sigSvc *util.SignService

	svc Server
}

func NewSignService(key *ecdsa.PrivateKey, svc Server) Server {
	return &signService{
		sigSvc: util.NewUnarySignService(key),
		svc:    svc,
	}
}

func (s *signService) AnnounceLocalTrust(ctx context.Context, req *reputation.AnnounceLocalTrustRequest) (*reputation.AnnounceLocalTrustResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.AnnounceLocalTrust(ctx, req.(*reputation.AnnounceLocalTrustRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*reputation.AnnounceLocalTrustResponse), nil
}

func (s *signService) AnnounceIntermediateResult(ctx context.Context, req *reputation.AnnounceIntermediateResultRequest) (*reputation.AnnounceIntermediateResultResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.AnnounceIntermediateResult(ctx, req.(*reputation.AnnounceIntermediateResultRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*reputation.AnnounceIntermediateResultResponse), nil
}
