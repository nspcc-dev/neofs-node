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
		func(ctx context.Context, req any) (util.ResponseMessage, error) {
			return s.svc.LocalNodeInfo(ctx, req.(*netmap.LocalNodeInfoRequest))
		},
		func() util.ResponseMessage {
			return new(netmap.LocalNodeInfoResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*netmap.LocalNodeInfoResponse), nil
}

func (s *signService) NetworkInfo(ctx context.Context, req *netmap.NetworkInfoRequest) (*netmap.NetworkInfoResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req any) (util.ResponseMessage, error) {
			return s.svc.NetworkInfo(ctx, req.(*netmap.NetworkInfoRequest))
		},
		func() util.ResponseMessage {
			return new(netmap.NetworkInfoResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*netmap.NetworkInfoResponse), nil
}

func (s *signService) Snapshot(ctx context.Context, req *netmap.SnapshotRequest) (*netmap.SnapshotResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req any) (util.ResponseMessage, error) {
			return s.svc.Snapshot(ctx, req.(*netmap.SnapshotRequest))
		},
		func() util.ResponseMessage {
			return new(netmap.SnapshotResponse)
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*netmap.SnapshotResponse), nil
}
