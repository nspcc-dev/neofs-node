package crypto

import (
	"crypto/ecdsa"
	"errors"
	"fmt"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// AuthenticateObject checks whether obj is signed correctly by its owner.
func AuthenticateObject(obj object.Object) error {
	sig := obj.Signature()
	if sig == nil {
		return errMissingSignature
	}

	var ecdsaPub *ecdsa.PublicKey
	scheme := sig.Scheme()
	switch scheme {
	default:
		return fmt.Errorf("unsupported scheme %v", scheme)
	case neofscrypto.ECDSA_SHA512, neofscrypto.ECDSA_DETERMINISTIC_SHA256, neofscrypto.ECDSA_WALLETCONNECT:
		var err error
		if ecdsaPub, err = decodeECDSAPublicKey(sig.PublicKeyBytes()); err != nil {
			return schemeError(scheme, fmt.Errorf("decode public key: %w", err))
		}
	}

	sessionToken := obj.SessionToken()
	if sessionToken != nil {
		// NOTE: update this place for non-ECDSA schemes
		if !sessionToken.AssertAuthKey((*neofsecdsa.PublicKey)(ecdsaPub)) { // same format for all ECDSA schemes
			return errors.New("session token is not for object's signer")
		}
		if err := AuthenticateToken(sessionToken); err != nil {
			return fmt.Errorf("session token: %w", err)
		}
		if sessionToken.Issuer() != obj.Owner() {
			return fmt.Errorf("different object owner and session issuer")
		}
	}

	switch scheme {
	default:
		panic(fmt.Sprintf("unexpected scheme %v", scheme)) // see switch above
	case neofscrypto.ECDSA_SHA512, neofscrypto.ECDSA_DETERMINISTIC_SHA256, neofscrypto.ECDSA_WALLETCONNECT:
		if !verifyECDSAFns[scheme](*ecdsaPub, sig.Value(), obj.GetID().Marshal()) {
			return schemeError(scheme, errSignatureMismatch)
		}
	}

	return nil
}
