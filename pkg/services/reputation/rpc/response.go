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

func (s *responseService) AnnounceLocalTrust(ctx context.Context, req *reputation.AnnounceLocalTrustRequest) (*reputation.AnnounceLocalTrustResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.AnnounceLocalTrust(ctx, req.(*reputation.AnnounceLocalTrustRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*reputation.AnnounceLocalTrustResponse), nil
}

func (s *responseService) AnnounceIntermediateResult(ctx context.Context, req *reputation.AnnounceIntermediateResultRequest) (*reputation.AnnounceIntermediateResultResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.AnnounceIntermediateResult(ctx, req.(*reputation.AnnounceIntermediateResultRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*reputation.AnnounceIntermediateResultResponse), nil
}
