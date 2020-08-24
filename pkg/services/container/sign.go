package container

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

type signService struct {
	putSigService     *util.UnarySignService
	getSigService     *util.UnarySignService
	delSigService     *util.UnarySignService
	listSigService    *util.UnarySignService
	setEACLSigService *util.UnarySignService
	getEACLSigService *util.UnarySignService
}

func NewSignService(key *ecdsa.PrivateKey, svc container.Service) container.Service {
	return &signService{
		putSigService: util.NewUnarySignService(
			key,
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return svc.Put(ctx, req.(*container.PutRequest))
			},
		),
		getSigService: util.NewUnarySignService(
			key,
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return svc.Get(ctx, req.(*container.GetRequest))
			},
		),
		delSigService: util.NewUnarySignService(
			key,
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return svc.Delete(ctx, req.(*container.DeleteRequest))
			},
		),
		listSigService: util.NewUnarySignService(
			key,
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return svc.List(ctx, req.(*container.ListRequest))
			},
		),
		setEACLSigService: util.NewUnarySignService(
			key,
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return svc.SetExtendedACL(ctx, req.(*container.SetExtendedACLRequest))
			},
		),
		getEACLSigService: util.NewUnarySignService(
			key,
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return svc.GetExtendedACL(ctx, req.(*container.GetExtendedACLRequest))
			},
		),
	}
}

func (s *signService) Put(ctx context.Context, req *container.PutRequest) (*container.PutResponse, error) {
	resp, err := s.putSigService.HandleUnaryRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.(*container.PutResponse), nil
}

func (s *signService) Delete(ctx context.Context, req *container.DeleteRequest) (*container.DeleteResponse, error) {
	resp, err := s.delSigService.HandleUnaryRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.(*container.DeleteResponse), nil
}

func (s *signService) Get(ctx context.Context, req *container.GetRequest) (*container.GetResponse, error) {
	resp, err := s.getSigService.HandleUnaryRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.(*container.GetResponse), nil
}

func (s *signService) List(ctx context.Context, req *container.ListRequest) (*container.ListResponse, error) {
	resp, err := s.listSigService.HandleUnaryRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.(*container.ListResponse), nil
}

func (s *signService) SetExtendedACL(ctx context.Context, req *container.SetExtendedACLRequest) (*container.SetExtendedACLResponse, error) {
	resp, err := s.setEACLSigService.HandleUnaryRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.(*container.SetExtendedACLResponse), nil
}

func (s *signService) GetExtendedACL(ctx context.Context, req *container.GetExtendedACLRequest) (*container.GetExtendedACLResponse, error) {
	resp, err := s.getEACLSigService.HandleUnaryRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.(*container.GetExtendedACLResponse), nil
}
