package netmap

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
)

type responseService struct {
	respSvc *response.Service

	svc netmap.Service
}

// NewResponseService returns netmap service instance that passes internal service
// call to response service.
func NewResponseService(nmSvc netmap.Service, respSvc *response.Service) netmap.Service {
	return &responseService{
		respSvc: respSvc,
		svc:     nmSvc,
	}
}

func (s *responseService) LocalNodeInfo(ctx context.Context, req *netmap.LocalNodeInfoRequest) (*netmap.LocalNodeInfoResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.LocalNodeInfo(ctx, req.(*netmap.LocalNodeInfoRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*netmap.LocalNodeInfoResponse), nil
}
