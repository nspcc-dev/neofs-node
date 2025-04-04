package crypto

import (
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
)

// VerifyRequestSignatures checks whether all request signatures are set and
// valid.
func VerifyRequestSignatures[B neofscrypto.ProtoMessage](req neofscrypto.SignedRequest[B]) error {
	return neofscrypto.VerifyRequestWithBuffer(req, nil)
}
