package util

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/pkg/errors"
)

type UnaryHandler func(context.Context, interface{}) (interface{}, error)

type UnarySignService struct {
	key *ecdsa.PrivateKey

	unaryHandler UnaryHandler
}

func NewUnarySignService(key *ecdsa.PrivateKey, handler UnaryHandler) *UnarySignService {
	return &UnarySignService{
		key:          key,
		unaryHandler: handler,
	}
}

func (s *UnarySignService) HandleUnaryRequest(ctx context.Context, req interface{}) (interface{}, error) {
	// verify request signatures
	if err := signature.VerifyServiceMessage(req); err != nil {
		return nil, errors.Wrap(err, "could not verify request")
	}

	// process request
	resp, err := s.unaryHandler(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "could not handle request")
	}

	// sign the response
	if err := signature.SignServiceMessage(s.key, resp); err != nil {
		return nil, errors.Wrap(err, "could not sign response")
	}

	return resp, nil
}
