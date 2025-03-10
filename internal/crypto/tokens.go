package crypto

import (
	"errors"
	"fmt"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// TODO: https://github.com/nspcc-dev/neofs-node/issues/2795 after API stabilization, move some components to SDK

// VerifyTokenSignature checks whether t is signed correctly and returns
// signatory user.
func VerifyTokenSignature[T interface {
	SignedData() []byte
	Signature() (neofscrypto.Signature, bool)
}](t T) (user.ID, error) {
	sig, ok := t.Signature()
	if !ok {
		return user.ID{}, errors.New("missing signature")
	}
	switch scheme := sig.Scheme(); scheme {
	default:
		return user.ID{}, fmt.Errorf("unsupported scheme %v", scheme)
	case neofscrypto.ECDSA_SHA512:
		pub, err := verifyECDSASHA512Signature(sig.PublicKeyBytes(), sig.Value(), t.SignedData)
		if err != nil {
			return user.ID{}, schemeError(neofscrypto.ECDSA_SHA512, err)
		}
		return user.NewFromECDSAPublicKey(*pub), nil
	case neofscrypto.ECDSA_DETERMINISTIC_SHA256:
		pub, err := verifyECDSARFC6979Signature(sig.PublicKeyBytes(), sig.Value(), t.SignedData)
		if err != nil {
			return user.ID{}, schemeError(neofscrypto.ECDSA_DETERMINISTIC_SHA256, err)
		}
		return user.NewFromECDSAPublicKey(*pub), nil
	case neofscrypto.ECDSA_WALLETCONNECT:
		pub, err := verifyECDSAWalletConnectSignature(sig.PublicKeyBytes(), sig.Value(), t.SignedData)
		if err != nil {
			return user.ID{}, schemeError(neofscrypto.ECDSA_WALLETCONNECT, err)
		}
		return user.NewFromECDSAPublicKey(*pub), nil
	}
}
