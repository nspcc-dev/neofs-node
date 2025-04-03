package crypto

import (
	"errors"
	"fmt"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// TODO: https://github.com/nspcc-dev/neofs-node/issues/2795 after API stabilization, move some components to SDK

// AuthenticateToken checks whether t is signed correctly by its issuer.
func AuthenticateToken[T interface {
	SignedData() []byte
	Signature() (neofscrypto.Signature, bool)
	Issuer() user.ID
}](token T) error {
	issuer := token.Issuer()
	if issuer.IsZero() {
		return errors.New("missing issuer")
	}
	sig, ok := token.Signature()
	if !ok {
		return errMissingSignature
	}
	switch scheme := sig.Scheme(); scheme {
	default:
		return fmt.Errorf("unsupported scheme %v", scheme)
	case neofscrypto.ECDSA_SHA512, neofscrypto.ECDSA_DETERMINISTIC_SHA256, neofscrypto.ECDSA_WALLETCONNECT:
		pub, err := decodeECDSAPublicKey(sig.PublicKeyBytes())
		if err != nil {
			return schemeError(scheme, fmt.Errorf("decode public key: %w", err))
		}
		if !verifyECDSAFns[scheme](*pub, sig.Value(), token.SignedData()) {
			return schemeError(scheme, errSignatureMismatch)
		}
		if user.NewFromECDSAPublicKey(*pub) != issuer {
			return errIssuerMismatch
		}
	}
	return nil
}
