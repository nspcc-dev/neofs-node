package netmap

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
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

func (s *signService) LocalNodeInfo(
	ctx context.Context,
	req *netmap.LocalNodeInfoRequest) (*netmap.LocalNodeInfoResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.LocalNodeInfo(ctx, req.(*netmap.LocalNodeInfoRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*netmap.LocalNodeInfoResponse), nil
}

func (s *signService) NetworkInfo(ctx context.Context, req *netmap.NetworkInfoRequest) (*netmap.NetworkInfoResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.NetworkInfo(ctx, req.(*netmap.NetworkInfoRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*netmap.NetworkInfoResponse), nil
}
