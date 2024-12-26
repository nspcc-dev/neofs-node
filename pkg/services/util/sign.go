package util

import (
	"crypto/ecdsa"
	"errors"

	"github.com/nspcc-dev/neofs-api-go/v2/rpc/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/signature"
	protostatus "github.com/nspcc-dev/neofs-api-go/v2/status/grpc"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"google.golang.org/protobuf/proto"
)

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
	// unwrap error
	for e := errors.Unwrap(err); e != nil; e = errors.Unwrap(err) {
		err = e
	}
	return apistatus.ErrorToV2(err).ToGRPCMessage().(*protostatus.Status)
}

// ToRequestSignatureVerificationError constructs status error describing
// request signature verification failure with the given cause.
func ToRequestSignatureVerificationError(cause error) error {
	var err apistatus.SignatureVerification
	err.SetMessage(cause.Error())
	return err
}
