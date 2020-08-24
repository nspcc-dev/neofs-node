package accounting

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

type signService struct {
	unarySigService *util.UnarySignService
}

func NewSignService(key *ecdsa.PrivateKey, svc accounting.Service) accounting.Service {
	return &signService{
		unarySigService: util.NewUnarySignService(
			key,
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return svc.Balance(ctx, req.(*accounting.BalanceRequest))
			},
		),
	}
}

func (s *signService) Balance(ctx context.Context, req *accounting.BalanceRequest) (*accounting.BalanceResponse, error) {
	resp, err := s.unarySigService.HandleUnaryRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.(*accounting.BalanceResponse), nil
}
