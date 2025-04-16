package crypto

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// TODO: https://github.com/nspcc-dev/neofs-node/issues/2795 after API stabilization, move some components to SDK

// AuthenticateToken checks whether t is signed correctly by its issuer.
//
// If signature scheme is unsupported, [ErrUnsupportedScheme] returns. It also
// returns when [neofscrypto.N3] scheme is used but fsChain is not provided.
func AuthenticateToken[T interface {
	SignedData() []byte
	Signature() (neofscrypto.Signature, bool)
	Issuer() user.ID
	Iat() uint64
}](token T, fsChain HistoricN3ScriptRunner) error {
	issuer := token.Issuer()
	if issuer.IsZero() {
		return errors.New("missing issuer")
	}
	sig, ok := token.Signature()
	if !ok {
		return errMissingSignature
	}
	var signer user.ID
	switch scheme := sig.Scheme(); scheme {
	default:
		return ErrUnsupportedScheme(scheme)
	case neofscrypto.ECDSA_SHA512, neofscrypto.ECDSA_DETERMINISTIC_SHA256, neofscrypto.ECDSA_WALLETCONNECT:
		pub, err := decodeECDSAPublicKey(sig.PublicKeyBytes())
		if err != nil {
			return schemeError(scheme, fmt.Errorf("decode public key: %w", err))
		}
		if !verifyECDSAFns[scheme](*pub, sig.Value(), token.SignedData()) {
			return schemeError(scheme, errSignatureMismatch)
		}
		signer = user.NewFromECDSAPublicKey(*pub)
	case neofscrypto.N3:
		if fsChain == nil {
			return ErrUnsupportedScheme(neofscrypto.N3)
		}
		verifScript := sig.PublicKeyBytes()
		verifScriptHash := hash.Hash160(verifScript) // TODO: or cut issuer?
		if err := verifyN3ScriptsAtEpoch(fsChain, token.Iat(), verifScriptHash, sig.Value(), verifScript, func() [sha256.Size]byte {
			return sha256.Sum256(token.SignedData())
		}); err != nil {
			return err
		}
		signer = user.NewFromScriptHash(verifScriptHash) // TODO: or check signer before?
	}
	if signer != issuer {
		return errIssuerMismatch
	}
	return nil
}
