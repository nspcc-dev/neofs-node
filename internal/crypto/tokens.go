package crypto

import (
	"errors"

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
	pub, err := verifySignature(sig, token.SignedData)
	if err != nil {
		return err
	}
	if user.NewFromECDSAPublicKey(*pub) != issuer {
		return errIssuerMismatch
	}
	return nil
}
