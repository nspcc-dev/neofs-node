package crypto

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"fmt"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// AuthenticateObject checks whether obj is signed correctly by its owner.
func AuthenticateObject(obj object.Object, fsChain HistoricN3ScriptRunner) error {
	sig := obj.Signature()
	if sig == nil {
		return errMissingSignature
	}

	var ecdsaPub *ecdsa.PublicKey
	scheme := sig.Scheme()
	sessionToken := obj.SessionToken()
	switch scheme {
	default:
		return fmt.Errorf("unsupported scheme %v", scheme)
	case neofscrypto.ECDSA_SHA512, neofscrypto.ECDSA_DETERMINISTIC_SHA256, neofscrypto.ECDSA_WALLETCONNECT:
		var err error
		if ecdsaPub, err = decodeECDSAPublicKey(sig.PublicKeyBytes()); err != nil {
			return schemeError(scheme, fmt.Errorf("decode public key: %w", err))
		}
	case neofscrypto.N3:
		if sessionToken != nil {
			// https://github.com/nspcc-dev/neofs-api/issues/305#issuecomment-2775087206
			return fmt.Errorf("%s scheme is not supported for objects created with session", scheme)
		}
	}

	if sessionToken != nil {
		// NOTE: update this place for non-ECDSA schemes
		if !sessionToken.AssertAuthKey((*neofsecdsa.PublicKey)(ecdsaPub)) { // same format for all ECDSA schemes
			return errors.New("session token is not for object's signer")
		}
		if err := AuthenticateToken(sessionToken, fsChain); err != nil {
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
		if sessionToken == nil && user.NewFromECDSAPublicKey(*ecdsaPub) != obj.Owner() {
			return errors.New("owner mismatches signature")
		}
	case neofscrypto.N3:
		if err := verifyN3ScriptsAtEpoch(fsChain, obj.CreationEpoch(), obj.Owner().ScriptHash(), sig.Value(), sig.PublicKeyBytes(), func() [sha256.Size]byte {
			return sha256.Sum256(obj.SignedData())
		}); err != nil {
			return err
		}
	}

	return nil
}
