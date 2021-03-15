package accounting

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
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

func (s *signService) Balance(ctx context.Context, req *accounting.BalanceRequest) (*accounting.BalanceResponse, error) {
	resp, err := s.sigSvc.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (util.ResponseMessage, error) {
			return s.svc.Balance(ctx, req.(*accounting.BalanceRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*accounting.BalanceResponse), nil
}
