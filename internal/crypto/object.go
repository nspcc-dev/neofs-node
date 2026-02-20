package crypto

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/version"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// AuthenticateObject checks whether obj is signed correctly by its owner.
func AuthenticateObject(obj object.Object, fsChain HistoricN3ScriptRunner, ecPart bool, resolver session.NNSResolver) error {
	sig := obj.Signature()
	if sig == nil {
		return errMissingSignature
	}

	var ecdsaPub *ecdsa.PublicKey
	scheme := sig.Scheme()
	sessionToken := obj.SessionToken()
	sessionTokenV2 := obj.SessionTokenV2()
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
			return errors.New("different object owner and session issuer")
		}
	}
	if sessionTokenV2 != nil {
		// NOTE: update this place for non-ECDSA schemes
		if ecdsaPub != nil {
			nodeUser := user.NewFromECDSAPublicKey(*ecdsaPub)
			ok, err := sessionTokenV2.AssertAuthority(nodeUser, resolver)
			if err != nil {
				return fmt.Errorf("assert session v2 authority: %w", err)
			}
			if !ok { // same format for all ECDSA schemes
				return errors.New("session v2 token is not for object's signer")
			}
		}
		if err := AuthenticateTokenV2(sessionTokenV2, fsChain); err != nil {
			return fmt.Errorf("session token v2: %w", err)
		}
		if sessionTokenV2.OriginalIssuer() != obj.Owner() {
			return errors.New("different object owner and session v2 issuer")
		}
	}

	switch scheme {
	default:
		panic(fmt.Sprintf("unexpected scheme %v", scheme)) // see switch above
	case neofscrypto.ECDSA_SHA512, neofscrypto.ECDSA_DETERMINISTIC_SHA256, neofscrypto.ECDSA_WALLETCONNECT:
		if !verifyECDSAFns[scheme](*ecdsaPub, sig.Value(), obj.GetID().Marshal()) {
			return schemeError(scheme, errSignatureMismatch)
		}
		if sessionToken == nil && sessionTokenV2 == nil && !ecPart &&
			user.NewFromECDSAPublicKey(*ecdsaPub) != obj.Owner() &&
			version.OwnerSignatureMatchRequired(obj.Version()) {
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
