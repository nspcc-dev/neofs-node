package reputationrpc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
)

type responseService struct {
	respSvc *response.Service

	svc Server
}

// NewResponseService returns reputation service server instance that passes
// internal service call to response service.
func NewResponseService(cnrSvc Server, respSvc *response.Service) Server {
	return &responseService{
		respSvc: respSvc,
		svc:     cnrSvc,
	}
}

func (s *responseService) SendLocalTrust(ctx context.Context, req *reputation.SendLocalTrustRequest) (*reputation.SendLocalTrustResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.SendLocalTrust(ctx, req.(*reputation.SendLocalTrustRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*reputation.SendLocalTrustResponse), nil
}

func (s *responseService) SendIntermediateResult(ctx context.Context, req *reputation.SendIntermediateResultRequest) (*reputation.SendIntermediateResultResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.SendIntermediateResult(ctx, req.(*reputation.SendIntermediateResultRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*reputation.SendIntermediateResultResponse), nil
}
