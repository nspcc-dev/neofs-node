package crypto

import (
	"fmt"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
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

// AuthenticateRequest checks whether all request signatures are set and valid.
// Returns request author ID along with public key if so, or
// [apistatus.SignatureVerification] error otherwise.
func AuthenticateRequest[B neofscrypto.ProtoMessage](req neofscrypto.SignedRequest[B]) (user.ID, []byte, error) {
	if err := VerifyRequestSignatures(req); err != nil {
		return user.ID{}, nil, err
	}

	var sig *refs.Signature
	for vh := req.GetVerifyHeader(); ; vh = vh.Origin {
		if vh.Origin == nil {
			sig = vh.BodySignature
			break
		}
	}
	// signature is non-nil here, otherwise VerifyRequestSignatures would have failed
	switch sig.Scheme {
	default:
		// can only happen if VerifyRequestSignatures supports more schemes than this func.
		// In practice, they are in sync, but still we must not panic
		return user.ID{}, nil, fmt.Errorf("unsupported scheme %v", sig.Scheme)
	case refs.SignatureScheme_ECDSA_SHA512, refs.SignatureScheme_ECDSA_RFC6979_SHA256, refs.SignatureScheme_ECDSA_RFC6979_SHA256_WALLET_CONNECT:
		// TODO: duplicates VerifyRequestSignatures action, think how to deduplicate
		ecdsaPub, _ := decodeECDSAPublicKey(sig.Key)
		return user.NewFromECDSAPublicKey(*ecdsaPub), sig.Key, nil
	}
}
