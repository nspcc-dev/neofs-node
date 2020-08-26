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
}

type ServerStreamHandler func(context.Context, interface{}) (MessageReader, error)

type MessageReader func() (interface{}, error)

type MessageStreamer struct {
	key *ecdsa.PrivateKey

	recv MessageReader
}

func NewUnarySignService(key *ecdsa.PrivateKey) *UnarySignService {
	return &UnarySignService{
		key: key,
	}
}

func (s *MessageStreamer) Recv() (interface{}, error) {
	m, err := s.recv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response message for signing")
	}

	if err := signature.SignServiceMessage(s.key, m); err != nil {
		return nil, errors.Wrap(err, "could not sign response message")
	}

	return m, nil
}

func (s *UnarySignService) HandleServerStreamRequest(ctx context.Context, req interface{}, handler ServerStreamHandler) (*MessageStreamer, error) {
	// verify request signatures
	if err := signature.VerifyServiceMessage(req); err != nil {
		return nil, errors.Wrap(err, "could not verify request")
	}

	msgRdr, err := handler(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "could not create message reader")
	}

	return &MessageStreamer{
		key:  s.key,
		recv: msgRdr,
	}, nil
}

func (s *UnarySignService) HandleUnaryRequest(ctx context.Context, req interface{}, handler UnaryHandler) (interface{}, error) {
	// verify request signatures
	if err := signature.VerifyServiceMessage(req); err != nil {
		return nil, errors.Wrap(err, "could not verify request")
	}

	// process request
	resp, err := handler(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "could not handle request")
	}

	// sign the response
	if err := signature.SignServiceMessage(s.key, resp); err != nil {
		return nil, errors.Wrap(err, "could not sign response")
	}

	return resp, nil
}
