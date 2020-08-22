package accounting

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/accounting"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/pkg/errors"
)

type signService struct {
	key *ecdsa.PrivateKey

	svc accounting.Service
}

func NewSignService(key *ecdsa.PrivateKey, svc accounting.Service) accounting.Service {
	return &signService{
		key: key,
		svc: svc,
	}
}

func (s *signService) Balance(ctx context.Context, req *accounting.BalanceRequest) (*accounting.BalanceResponse, error) {
	// verify request signatures
	if err := signature.VerifyServiceMessage(req); err != nil {
		return nil, errors.Wrap(err, "could not verify request")
	}

	// process request
	resp, err := s.svc.Balance(ctx, req)
	if err != nil {
		return nil, err
	}

	// sign the response
	if err := signature.SignServiceMessage(s.key, resp); err != nil {
		return nil, errors.Wrap(err, "could not sign response")
	}

	return resp, nil
}
