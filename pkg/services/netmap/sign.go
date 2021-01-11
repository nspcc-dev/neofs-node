package netmap

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

type signService struct {
	sigSvc *util.SignService

	svc netmap.Service
}

func NewSignService(key *ecdsa.PrivateKey, svc netmap.Service) netmap.Service {
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
