package crypto

import (
	"errors"
	"fmt"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// VerifyRequestSignatures checks whether all request signatures are set and
// valid. Returns [apistatus.SignatureVerification] otherwise.
func VerifyRequestSignatures[B neofscrypto.ProtoMessage](req neofscrypto.SignedRequest[B]) error {
	err := neofscrypto.VerifyRequestWithBuffer(req, nil)
	if err != nil {
		var st apistatus.SignatureVerification
		st.SetMessage(err.Error())
		return st
	}
	return nil
}

// GetRequestAuthor returns ID of the request author along with public key from
// the request verification header.
func GetRequestAuthor(vh *protosession.RequestVerificationHeader) (user.ID, []byte, error) {
	if vh == nil {
		return user.ID{}, nil, errors.New("missing verification header")
	}

	var sig *refs.Signature
	for ; ; vh = vh.Origin {
		if vh.Origin == nil {
			sig = vh.BodySignature
			break
		}
	}
	if sig == nil {
		return user.ID{}, nil, errors.New("missing body signature")
	}

	switch sig.Scheme {
	default:
		return user.ID{}, nil, fmt.Errorf("unsupported scheme %v", sig.Scheme)
	case refs.SignatureScheme_ECDSA_SHA512, refs.SignatureScheme_ECDSA_RFC6979_SHA256, refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT:
		// TODO: being called with VerifyRequestSignatures, public key is decoded twice.
		//  Not so big overhead, but still better to avoid this
		ecdsaPub, _ := decodeECDSAPublicKey(sig.Key)
		return user.NewFromECDSAPublicKey(*ecdsaPub), sig.Key, nil
	}
}
