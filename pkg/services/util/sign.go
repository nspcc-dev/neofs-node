package util

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/pkg/errors"
)

// ResponseMessage is an interface of NeoFS response message.
type ResponseMessage interface {
	GetMetaHeader() *session.ResponseMetaHeader
	SetMetaHeader(*session.ResponseMetaHeader)
}

type UnaryHandler func(context.Context, interface{}) (ResponseMessage, error)

type SignService struct {
	key *ecdsa.PrivateKey
}

type ServerStreamHandler func(context.Context, interface{}) (ResponseMessageReader, error)

type ResponseMessageReader func() (ResponseMessage, error)

type ResponseMessageStreamer struct {
	key *ecdsa.PrivateKey

	recv ResponseMessageReader
}

type RequestMessageWriter func(interface{}) error

type ClientStreamCloser func() (ResponseMessage, error)

type RequestMessageStreamer struct {
	key *ecdsa.PrivateKey

	send RequestMessageWriter

	close ClientStreamCloser
}

func NewUnarySignService(key *ecdsa.PrivateKey) *SignService {
	return &SignService{
		key: key,
	}
}

func (s *RequestMessageStreamer) Send(req interface{}) error {
	// verify request signatures
	if err := signature.VerifyServiceMessage(req); err != nil {
		return errors.Wrap(err, "could not verify request")
	}

	return s.send(req)
}

func (s *RequestMessageStreamer) CloseAndRecv() (ResponseMessage, error) {
	resp, err := s.close()
	if err != nil {
		return nil, errors.Wrap(err, "could not close stream and receive response")
	}

	if err := signature.SignServiceMessage(s.key, resp); err != nil {
		return nil, errors.Wrap(err, "could not sign response")
	}

	return resp, nil
}

func (s *SignService) CreateRequestStreamer(sender RequestMessageWriter, closer ClientStreamCloser) *RequestMessageStreamer {
	return &RequestMessageStreamer{
		key:   s.key,
		send:  sender,
		close: closer,
	}
}

func (s *ResponseMessageStreamer) Recv() (ResponseMessage, error) {
	m, err := s.recv()
	if err != nil {
		return nil, errors.Wrap(err, "could not receive response message for signing")
	}

	if err := signature.SignServiceMessage(s.key, m); err != nil {
		return nil, errors.Wrap(err, "could not sign response message")
	}

	return m, nil
}

func (s *SignService) HandleServerStreamRequest(ctx context.Context, req interface{}, handler ServerStreamHandler) (*ResponseMessageStreamer, error) {
	// verify request signatures
	if err := signature.VerifyServiceMessage(req); err != nil {
		return nil, errors.Wrap(err, "could not verify request")
	}

	msgRdr, err := handler(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "could not create message reader")
	}

	return &ResponseMessageStreamer{
		key:  s.key,
		recv: msgRdr,
	}, nil
}

func (s *SignService) HandleUnaryRequest(ctx context.Context, req interface{}, handler UnaryHandler) (ResponseMessage, error) {
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
