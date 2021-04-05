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

func (s *signService) SendLocalTrust(ctx context.Context, req *reputation.SendLocalTrustRequest) (*reputation.SendLocalTrustResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.SendLocalTrust(ctx, req.(*reputation.SendLocalTrustRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*reputation.SendLocalTrustResponse), nil
}

func (s *signService) SendIntermediateResult(ctx context.Context, req *reputation.SendIntermediateResultRequest) (*reputation.SendIntermediateResultResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.SendIntermediateResult(ctx, req.(*reputation.SendIntermediateResultRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*reputation.SendIntermediateResultResponse), nil
}
