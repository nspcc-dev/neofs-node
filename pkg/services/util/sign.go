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

type UnaryHandler func(context.Context, interface{}) (ResponseMessage, error)

type SignService struct {
	key *ecdsa.PrivateKey
}

type ResponseMessageWriter func(ResponseMessage) error

type ServerStreamHandler func(context.Context, interface{}) (ResponseMessageReader, error)

type ResponseMessageReader func() (ResponseMessage, error)

var ErrAbortStream = errors.New("abort message stream")

type ResponseConstructor func() ResponseMessage

type RequestMessageWriter func(interface{}) error

type ClientStreamCloser func() (ResponseMessage, error)

type RequestMessageStreamer struct {
	key *ecdsa.PrivateKey

	send RequestMessageWriter

	close ClientStreamCloser

	respCons ResponseConstructor

	statusSupported bool

	sendErr error
}

func NewUnarySignService(key *ecdsa.PrivateKey) *SignService {
	return &SignService{
		key: key,
	}
}

func (s *RequestMessageStreamer) Send(req interface{}) error {
	// req argument should be strengthen with type RequestMessage
	s.statusSupported = isStatusSupported(req.(RequestMessage)) // panic is OK here for now

	var err error

	// verify request signatures
	if err = signature.VerifyServiceMessage(req); err != nil {
		err = fmt.Errorf("could not verify request: %w", err)
	} else {
		err = s.send(req)
	}

	if err != nil {
		if !s.statusSupported {
			return err
		}

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
		if !s.statusSupported {
			return nil, err
		}

		var st apistatus.ServerInternal // specific API status should be set according to error

		apistatus.WriteInternalServerErr(&st, err)

		resp = s.respCons()

		setStatusV2(resp, st)
	}

	if err = signResponse(s.key, resp, s.statusSupported); err != nil {
		return nil, err
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
	req interface{},
	respWriter ResponseMessageWriter,
	blankResp ResponseConstructor,
	respWriterCaller func(ResponseMessageWriter) error,
) error {
	// handle protocol versions <=2.10 (API statuses was introduced in 2.11 only)

	// req argument should be strengthen with type RequestMessage
	statusSupported := isStatusSupported(req.(RequestMessage)) // panic is OK here for now

	var err error

	// verify request signatures
	if err = signature.VerifyServiceMessage(req); err != nil {
		err = fmt.Errorf("could not verify request: %w", err)
	} else {
		err = respWriterCaller(func(resp ResponseMessage) error {
			if err := signResponse(s.key, resp, statusSupported); err != nil {
				return err
			}

			return respWriter(resp)
		})
	}

	if err != nil {
		if !statusSupported {
			return err
		}

		var st apistatus.ServerInternal // specific API status should be set according to error

		apistatus.WriteInternalServerErr(&st, err)

		resp := blankResp()

		setStatusV2(resp, st)

		_ = signResponse(s.key, resp, false) // panics or returns nil with false arg

		return respWriter(resp)
	}

	return nil
}

func (s *SignService) HandleUnaryRequest(ctx context.Context, req interface{}, handler UnaryHandler, blankResp ResponseConstructor) (ResponseMessage, error) {
	// handle protocol versions <=2.10 (API statuses was introduced in 2.11 only)

	// req argument should be strengthen with type RequestMessage
	statusSupported := isStatusSupported(req.(RequestMessage)) // panic is OK here for now

	var (
		resp ResponseMessage
		err  error
	)

	// verify request signatures
	if err = signature.VerifyServiceMessage(req); err != nil {
		err = fmt.Errorf("could not verify request: %w", err)
	} else {
		// process request
		resp, err = handler(ctx, req)
	}

	if err != nil {
		if !statusSupported {
			return nil, err
		}

		var st apistatus.ServerInternal // specific API status should be set according to error

		apistatus.WriteInternalServerErr(&st, err)

		resp = blankResp()

		setStatusV2(resp, st)
	}

	// sign the response
	if err = signResponse(s.key, resp, statusSupported); err != nil {
		return nil, err
	}

	return resp, nil
}

func isStatusSupported(req RequestMessage) bool {
	version := req.GetMetaHeader().GetVersion()

	mjr := version.GetMajor()

	return mjr > 2 || mjr == 2 && version.GetMinor() >= 11
}

func setStatusV2(resp ResponseMessage, st apistatus.Status) {
	session.SetStatus(resp, apistatus.ToStatusV2(st))
}

// signs response with private key via signature.SignServiceMessage.
// The signature error affects the result depending on the protocol version:
//  * if status return is supported, panics since we cannot return the failed status, because it will not be signed;
//  * otherwise, returns error in order to transport it directly.
func signResponse(key *ecdsa.PrivateKey, resp interface{}, statusSupported bool) error {
	err := signature.SignServiceMessage(key, resp)
	if err != nil {
		err = fmt.Errorf("could not sign response: %w", err)

		if statusSupported {
			// We can't pass this error as status code since response will be unsigned.
			// Isn't expected in practice, so panic is ok here.
			panic(err)
		}
	}

	return err
}
