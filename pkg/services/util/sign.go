package util

import (
	"crypto/ecdsa"
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	sdkcrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	sdkecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
)

// Request is a common interface of API request messages.
type Request interface {
	GetMetaHeader() *protosession.RequestMetaHeader
}

// SignResponseIfNeeded checks whether response for the req should be signed. If
// so, calculated verification header is returned. Otherwise, nil returns.
func SignResponseIfNeeded[R sdkcrypto.ProtoMessage](signer *ecdsa.PrivateKey, r sdkcrypto.SignedResponse[R], req Request) *protosession.ResponseVerificationHeader {
	var ver *refs.Version
	for mh := req.GetMetaHeader(); mh != nil; mh = mh.Origin {
		if mh.Origin == nil {
			ver = mh.Version
			break
		}
	}

	// getters are NPE-protected
	if ver.GetMajor() > 2 || ver.GetMajor() == 2 && ver.GetMinor() >= 22 {
		return nil
	}

	return SignResponse(signer, r)
}

func SignResponse[R sdkcrypto.ProtoMessage](signer *ecdsa.PrivateKey, r sdkcrypto.SignedResponse[R]) *protosession.ResponseVerificationHeader {
	verHeader, err := sdkcrypto.SignResponseWithBuffer(sdkecdsa.Signer(*signer), r, nil)
	if err != nil {
		// We can't pass this error as NeoFS status code since response will be unsigned.
		// Isn't expected in practice, so panic is ok here.
		panic(err)
	}
	return verHeader
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
	return apistatus.FromError(err)
}
