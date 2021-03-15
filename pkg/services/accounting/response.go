package accounting

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
)

type responseService struct {
	respSvc *response.Service

	svc Server
}

// NewResponseService returns accounting service instance that passes internal service
// call to response service.
func NewResponseService(accSvc Server, respSvc *response.Service) Server {
	return &responseService{
		respSvc: respSvc,
		svc:     accSvc,
	}
}

func (s *responseService) Balance(ctx context.Context, req *accounting.BalanceRequest) (*accounting.BalanceResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Balance(ctx, req.(*accounting.BalanceRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*accounting.BalanceResponse), nil
}
