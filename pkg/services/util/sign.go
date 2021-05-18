package util

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
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

type ResponseMessageWriter func(ResponseMessage) error

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
		return fmt.Errorf("could not verify request: %w", err)
	}

	return s.send(req)
}

func (s *RequestMessageStreamer) CloseAndRecv() (ResponseMessage, error) {
	resp, err := s.close()
	if err != nil {
		return nil, fmt.Errorf("could not close stream and receive response: %w", err)
	}

	if err := signature.SignServiceMessage(s.key, resp); err != nil {
		return nil, fmt.Errorf("could not sign response: %w", err)
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
		return nil, fmt.Errorf("could not receive response message for signing: %w", err)
	}

	if err := signature.SignServiceMessage(s.key, m); err != nil {
		return nil, fmt.Errorf("could not sign response message: %w", err)
	}

	return m, nil
}

func (s *SignService) HandleServerStreamRequest(req interface{}, respWriter ResponseMessageWriter) (ResponseMessageWriter, error) {
	// verify request signatures
	if err := signature.VerifyServiceMessage(req); err != nil {
		return nil, fmt.Errorf("could not verify request: %w", err)
	}

	return func(resp ResponseMessage) error {
		if err := signature.SignServiceMessage(s.key, resp); err != nil {
			return fmt.Errorf("could not sign response message: %w", err)
		}

		return respWriter(resp)
	}, nil
}

func (s *SignService) HandleUnaryRequest(ctx context.Context, req interface{}, handler UnaryHandler) (ResponseMessage, error) {
	// verify request signatures
	if err := signature.VerifyServiceMessage(req); err != nil {
		return nil, fmt.Errorf("could not verify request: %w", err)
	}

	// process request
	resp, err := handler(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("could not handle request: %w", err)
	}

	// sign the response
	if err := signature.SignServiceMessage(s.key, resp); err != nil {
		return nil, fmt.Errorf("could not sign response: %w", err)
	}

	return resp, nil
}
