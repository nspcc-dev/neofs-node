package util

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

type RequestMessage interface {
	GetMetaHeader() *session.RequestMetaHeader
}

// ResponseMessage is an interface of NeoFS response message.
type ResponseMessage interface {
	GetMetaHeader() *session.ResponseMetaHeader
	SetMetaHeader(*session.ResponseMetaHeader)
}

type UnaryHandler func(context.Context, any) (ResponseMessage, error)

type SignService struct {
	key *ecdsa.PrivateKey
}

type ResponseMessageWriter func(ResponseMessage) error

type ServerStreamHandler func(context.Context, any) (ResponseMessageReader, error)

type ResponseMessageReader func() (ResponseMessage, error)

var ErrAbortStream = errors.New("abort message stream")

type ResponseConstructor func() ResponseMessage

type RequestMessageWriter func(any) error

type ClientStreamCloser func() (ResponseMessage, error)

type RequestMessageStreamer struct {
	key *ecdsa.PrivateKey

	send RequestMessageWriter

	close ClientStreamCloser

	respCons ResponseConstructor

	sendErr error
}

func NewUnarySignService(key *ecdsa.PrivateKey) *SignService {
	return &SignService{
		key: key,
	}
}

func (s *RequestMessageStreamer) Send(req any) error {
	var err error

	// verify request signatures
	if err = signature.VerifyServiceMessage(req); err != nil {
		err = fmt.Errorf("could not verify request: %w", err)
	} else {
		err = s.send(req)
	}

	if err != nil {
		s.sendErr = err

		return ErrAbortStream
	}

	return nil
}

func (s *RequestMessageStreamer) CloseAndRecv() (ResponseMessage, error) {
	var (
		resp ResponseMessage
		err  error
	)

	if s.sendErr != nil {
		err = s.sendErr
	} else {
		resp, err = s.close()
		if err != nil {
			err = fmt.Errorf("could not close stream and receive response: %w", err)
		}
	}

	if err != nil {
		resp = s.respCons()

		setStatusV2(resp, err)
	}

	if err = signature.SignServiceMessage(s.key, resp); err != nil {
		// We can't pass this error as status code since response will be unsigned.
		// Isn't expected in practice, so panic is ok here.
		panic(err)
	}

	return resp, nil
}

func (s *SignService) CreateRequestStreamer(sender RequestMessageWriter, closer ClientStreamCloser, blankResp ResponseConstructor) *RequestMessageStreamer {
	return &RequestMessageStreamer{
		key:   s.key,
		send:  sender,
		close: closer,

		respCons: blankResp,
	}
}

func (s *SignService) HandleServerStreamRequest(
	req any,
	respWriter ResponseMessageWriter,
	blankResp ResponseConstructor,
	respWriterCaller func(ResponseMessageWriter) error,
) error {
	var err error

	// verify request signatures
	if err = signature.VerifyServiceMessage(req); err != nil {
		err = fmt.Errorf("could not verify request: %w", err)
	} else {
		err = respWriterCaller(func(resp ResponseMessage) error {
			if err := signature.SignServiceMessage(s.key, resp); err != nil {
				// We can't pass this error as status code since response will be unsigned.
				// Isn't expected in practice, so panic is ok here.
				panic(err)
			}

			return respWriter(resp)
		})
	}

	if err != nil {
		resp := blankResp()

		setStatusV2(resp, err)

		_ = signature.SignServiceMessage(s.key, resp)

		return respWriter(resp)
	}

	return nil
}

func (s *SignService) HandleUnaryRequest(ctx context.Context, req any, handler UnaryHandler, blankResp ResponseConstructor) (ResponseMessage, error) {
	var (
		resp ResponseMessage
		err  error
	)

	// verify request signatures
	if err = signature.VerifyServiceMessage(req); err != nil {
		var sigErr apistatus.SignatureVerification
		sigErr.SetMessage(err.Error())

		err = sigErr
	} else {
		// process request
		resp, err = handler(ctx, req)
	}

	if err != nil {
		resp = blankResp()

		setStatusV2(resp, err)
	}

	// sign the response
	if err = signature.SignServiceMessage(s.key, resp); err != nil {
		// We can't pass this error as status code since response will be unsigned.
		// Isn't expected in practice, so panic is ok here.
		panic(err)
	}

	return resp, nil
}

func setStatusV2(resp ResponseMessage, err error) {
	// unwrap error
	for e := errors.Unwrap(err); e != nil; e = errors.Unwrap(err) {
		err = e
	}

	session.SetStatus(resp, apistatus.ErrorToV2(err))
}
