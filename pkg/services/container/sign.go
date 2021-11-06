package container

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
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

func (s *signService) Put(ctx context.Context, req *container.PutRequest) (*container.PutResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Put(ctx, req.(*container.PutRequest))
		},
		func() util.ResponseMessage {
			return new(container.PutResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.PutResponse), nil
}

func (s *signService) Delete(ctx context.Context, req *container.DeleteRequest) (*container.DeleteResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Delete(ctx, req.(*container.DeleteRequest))
		},
		func() util.ResponseMessage {
			return new(container.DeleteResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.DeleteResponse), nil
}

func (s *signService) Get(ctx context.Context, req *container.GetRequest) (*container.GetResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Get(ctx, req.(*container.GetRequest))
		},
		func() util.ResponseMessage {
			return new(container.GetResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.GetResponse), nil
}

func (s *signService) List(ctx context.Context, req *container.ListRequest) (*container.ListResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.List(ctx, req.(*container.ListRequest))
		},
		func() util.ResponseMessage {
			return new(container.ListResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.ListResponse), nil
}

func (s *signService) SetExtendedACL(ctx context.Context, req *container.SetExtendedACLRequest) (*container.SetExtendedACLResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.SetExtendedACL(ctx, req.(*container.SetExtendedACLRequest))
		},
		func() util.ResponseMessage {
			return new(container.SetExtendedACLResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.SetExtendedACLResponse), nil
}

func (s *signService) GetExtendedACL(ctx context.Context, req *container.GetExtendedACLRequest) (*container.GetExtendedACLResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.GetExtendedACL(ctx, req.(*container.GetExtendedACLRequest))
		},
		func() util.ResponseMessage {
			return new(container.GetExtendedACLResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.GetExtendedACLResponse), nil
}

func (s *signService) AnnounceUsedSpace(ctx context.Context, req *container.AnnounceUsedSpaceRequest) (*container.AnnounceUsedSpaceResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.AnnounceUsedSpace(ctx, req.(*container.AnnounceUsedSpaceRequest))
		},
		func() util.ResponseMessage {
			return new(container.AnnounceUsedSpaceResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*container.AnnounceUsedSpaceResponse), nil
}
