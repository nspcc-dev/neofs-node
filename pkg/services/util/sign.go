package util

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/v2/rpc/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	"google.golang.org/protobuf/proto"
)

// ResponseMessage is an interface of NeoFS response message.
type ResponseMessage interface {
	GetMetaHeader() *session.ResponseMetaHeader
	SetMetaHeader(*session.ResponseMetaHeader)
}

type UnaryHandler func(context.Context, any) (ResponseMessage, error)

type ResponseMessageWriter func(ResponseMessage) error

type ResponseMessageReader func() (ResponseMessage, error)

type RequestMessageWriter func(any) error

type ClientStreamCloser func() (ResponseMessage, error)

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
