package crypto

import (
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
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
