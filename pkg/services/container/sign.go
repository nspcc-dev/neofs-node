package container

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

type signService struct {
	sigSvc *util.SignService

	svc container.Service
}

func NewSignService(key *ecdsa.PrivateKey, svc container.Service) container.Service {
	return &signService{
		sigSvc: util.NewUnarySignService(key),
		svc:    svc,
	}
}

func (s *signService) Put(ctx context.Context, req *container.PutRequest) (*container.PutResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return s.svc.Put(ctx, req.(*container.PutRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.PutResponse), nil
}

func (s *signService) Delete(ctx context.Context, req *container.DeleteRequest) (*container.DeleteResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return s.svc.Get(ctx, req.(*container.GetRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.DeleteResponse), nil
}

func (s *signService) Get(ctx context.Context, req *container.GetRequest) (*container.GetResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return s.svc.Delete(ctx, req.(*container.DeleteRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.GetResponse), nil
}

func (s *signService) List(ctx context.Context, req *container.ListRequest) (*container.ListResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return s.svc.List(ctx, req.(*container.ListRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.ListResponse), nil
}

func (s *signService) SetExtendedACL(ctx context.Context, req *container.SetExtendedACLRequest) (*container.SetExtendedACLResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return s.svc.SetExtendedACL(ctx, req.(*container.SetExtendedACLRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.SetExtendedACLResponse), nil
}

func (s *signService) GetExtendedACL(ctx context.Context, req *container.GetExtendedACLRequest) (*container.GetExtendedACLResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return s.svc.GetExtendedACL(ctx, req.(*container.GetExtendedACLRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.GetExtendedACLResponse), nil
}
