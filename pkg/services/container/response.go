package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/util/response"
)

type responseService struct {
	respSvc *response.Service

	svc container.Service
}

// NewResponseService returns container service instance that passes internal service
// call to response service.
func NewResponseService(cnrSvc container.Service, respSvc *response.Service) container.Service {
	return &responseService{
		respSvc: respSvc,
		svc:     cnrSvc,
	}
}

func (s *responseService) Put(ctx context.Context, req *container.PutRequest) (*container.PutResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Put(ctx, req.(*container.PutRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.PutResponse), nil
}

func (s *responseService) Delete(ctx context.Context, req *container.DeleteRequest) (*container.DeleteResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Delete(ctx, req.(*container.DeleteRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.DeleteResponse), nil
}

func (s *responseService) Get(ctx context.Context, req *container.GetRequest) (*container.GetResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Get(ctx, req.(*container.GetRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.GetResponse), nil
}

func (s *responseService) List(ctx context.Context, req *container.ListRequest) (*container.ListResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.List(ctx, req.(*container.ListRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.ListResponse), nil
}

func (s *responseService) SetExtendedACL(ctx context.Context, req *container.SetExtendedACLRequest) (*container.SetExtendedACLResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.SetExtendedACL(ctx, req.(*container.SetExtendedACLRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.SetExtendedACLResponse), nil
}

func (s *responseService) GetExtendedACL(ctx context.Context, req *container.GetExtendedACLRequest) (*container.GetExtendedACLResponse, error) {
	resp, err := s.respSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.GetExtendedACL(ctx, req.(*container.GetExtendedACLRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.GetExtendedACLResponse), nil
}
