package session

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

type signService struct {
	unarySigService *util.UnarySignService

	svc session.Service
}

func NewSignService(key *ecdsa.PrivateKey, svc session.Service) session.Service {
	return &signService{
		unarySigService: util.NewUnarySignService(key),
	}
}

func (s *signService) Create(ctx context.Context, req *session.CreateRequest) (*session.CreateResponse, error) {
	resp, err := s.unarySigService.HandleUnaryRequest(ctx, req,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			return s.svc.Create(ctx, req.(*session.CreateRequest))
		},
	)
	if err != nil {
		return nil, err
	}

	return resp.(*session.CreateResponse), nil
}
