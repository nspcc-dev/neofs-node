package crypto

import (
	"crypto/ecdsa"
	"errors"
	"fmt"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// TODO: https://github.com/nspcc-dev/neofs-node/issues/2795 after API stabilization, move some components to SDK

// SignedToken is a common interface of signed NeoFS tokens.
type SignedToken interface {
	SignedData() []byte
	Signature() (neofscrypto.Signature, bool)
}

// AuthenticateToken checks whether t is signed correctly by its issuer.
func AuthenticateToken[T interface {
	SignedToken
	Issuer() user.ID
}](token T) error {
	return authToken(token, token.Issuer())
}

func authToken[T SignedToken](token T, issuer user.ID) error {
	if issuer.IsZero() {
		return errors.New("missing issuer")
	}
	pub, err := verifyTokenSignature(token)
	if err != nil {
		return err
	}
	if user.NewFromECDSAPublicKey(*pub) != issuer {
		return errIssuerMismatch
	}
	return nil
}

func verifyTokenSignature[T SignedToken](token T) (*ecdsa.PublicKey, error) {
	sig, ok := token.Signature()
	if !ok {
		return nil, errors.New("missing signature")
	}
	switch scheme := sig.Scheme(); scheme {
	default:
		return nil, fmt.Errorf("unsupported scheme %v", scheme)
	case neofscrypto.ECDSA_SHA512, neofscrypto.ECDSA_DETERMINISTIC_SHA256, neofscrypto.ECDSA_WALLETCONNECT:
		pub, err := verifyECDSAFns[scheme](sig.PublicKeyBytes(), sig.Value(), token.SignedData)
		if err != nil {
			return nil, schemeError(scheme, err)
		}
		return pub, nil
	}
}
