package crypto

import (
	"crypto/ecdsa"
	"crypto/sha256"

	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// authenticateContainerRequestRFC6979 checks whether given payload of the
// request of the container is signed correctly by its owner. Deterministic
// ECDSA with SHA-256 hashing (RFC 6979) algorithm is used.
func authenticateContainerRequestRFC6979(owner user.ID, pub *ecdsa.PublicKey, sig, payload []byte) error {
	if !verifyECDSARFC6979Signature(*pub, sig, payload) {
		return errSignatureMismatch
	}
	if user.NewFromECDSAPublicKey(*pub) != owner {
		return errOwnerMismatch
	}
	return nil
}

// AuthenticateContainerRequest checks N3 witness of the container owner sent
// along with specified request payload against the current FS chain state.
//
// If verifScript is a compressed ECDSA public key, AuthenticateContainerRequest
// switches to deterministic ECDSA with SHA-256 hashing (RFC 6979) algorithm.
func AuthenticateContainerRequest(owner user.ID, invocScript, verifScript, payload []byte, fsChain N3ScriptRunner) error {
	if pub, err := decodeECDSAPublicKey(verifScript); err == nil {
		return authenticateContainerRequestRFC6979(owner, pub, invocScript, payload)
	}

	return verifyN3ScriptsNow(fsChain, owner.ScriptHash(), invocScript, verifScript, func() [sha256.Size]byte {
		return sha256.Sum256(payload)
	})
}
