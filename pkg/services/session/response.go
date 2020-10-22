package session

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
)

type responseService struct {
	respSvc *response.Service

	svc session.Service
}

// NewResponseService returns session service instance that passes internal service
// call to response service.
func NewResponseService(ssSvc session.Service, respSvc *response.Service) session.Service {
	return &responseService{
		respSvc: respSvc,
		svc:     ssSvc,
	}
}

func (s *responseService) Create(ctx context.Context, req *session.CreateRequest) (*session.CreateResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Create(ctx, req.(*session.CreateRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*session.CreateResponse), nil
}
