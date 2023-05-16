package netmap

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
)

type responseService struct {
	respSvc *response.Service

	svc Server
}

// NewResponseService returns netmap service instance that passes internal service
// call to response service.
func NewResponseService(nmSvc Server, respSvc *response.Service) Server {
	return &responseService{
		respSvc: respSvc,
		svc:     nmSvc,
	}
}

func (s *responseService) LocalNodeInfo(ctx context.Context, req *netmap.LocalNodeInfoRequest) (*netmap.LocalNodeInfoResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req any) (util.ResponseMessage, error) {
			return s.svc.LocalNodeInfo(ctx, req.(*netmap.LocalNodeInfoRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*netmap.LocalNodeInfoResponse), nil
}

func (s *responseService) NetworkInfo(ctx context.Context, req *netmap.NetworkInfoRequest) (*netmap.NetworkInfoResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req any) (util.ResponseMessage, error) {
			return s.svc.NetworkInfo(ctx, req.(*netmap.NetworkInfoRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*netmap.NetworkInfoResponse), nil
}

func (s *responseService) Snapshot(ctx context.Context, req *netmap.SnapshotRequest) (*netmap.SnapshotResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req any) (util.ResponseMessage, error) {
			return s.svc.Snapshot(ctx, req.(*netmap.SnapshotRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*netmap.SnapshotResponse), nil
}
