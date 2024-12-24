package util

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/rpc/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"github.com/nspcc-dev/neofs-api-go/v2/status"
	protostatus "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"google.golang.org/protobuf/proto"
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

func SignResponse[R proto.Message, RV2 any, RV2PTR interface {
	*RV2
	ToGRPCMessage() grpc.Message
	FromGRPCMessage(message grpc.Message) error
}](signer *ecdsa.PrivateKey, r R, _ RV2) R {
	r2 := RV2PTR(new(RV2))
	if err := r2.FromGRPCMessage(r); err != nil {
		panic(err) // can only fail on wrong type, here it's correct
	}
	if err := signature.SignServiceMessage(signer, r2); err != nil {
		// We can't pass this error as NeoFS status code since response will be unsigned.
		// Isn't expected in practice, so panic is ok here.
		panic(err)
	}
	return r2.ToGRPCMessage().(R)
}

func (s *SignService) HandleUnaryRequest(ctx context.Context, req any, handler UnaryHandler, blankResp ResponseConstructor) (ResponseMessage, error) {
	var (
		resp ResponseMessage
		err  error
	)

	// verify request signatures
	if err = signature.VerifyServiceMessage(req); err != nil {
		err = ToRequestSignatureVerificationError(err)
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
	session.SetStatus(resp, statusFromErr(err))
}

func statusFromErr(err error) *status.Status {
	// unwrap error
	for e := errors.Unwrap(err); e != nil; e = errors.Unwrap(err) {
		err = e
	}
	return apistatus.ErrorToV2(err)
}

var (
	// StatusOK is a missing response status field meaning OK in NeoFS protocol. It
	// allows to make code more clear instead of passing nil.
	StatusOK *protostatus.Status
	// StatusOKErr is an error corresponding to [StatusOK]. It allows to make code
	// more clear instead of passing nil.
	StatusOKErr error
)

// ToStatus unwraps the deepest error from err and converts it into the response
// status.
func ToStatus(err error) *protostatus.Status {
	return statusFromErr(err).ToGRPCMessage().(*protostatus.Status)
}

// ToRequestSignatureVerificationError constructs status error describing
// request signature verification failure with the given cause.
func ToRequestSignatureVerificationError(cause error) error {
	var err apistatus.SignatureVerification
	err.SetMessage(cause.Error())
	return err
}
