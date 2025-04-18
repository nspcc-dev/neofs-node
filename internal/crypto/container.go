package crypto

import (
	"crypto/sha256"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// authenticateContainerRequestRFC6979 checks whether given payload of the
// request of the container is signed correctly by its owner. Deterministic
// ECDSA with SHA-256 hashing (RFC 6979) algorithm is used.
func authenticateContainerRequestRFC6979(owner user.ID, pubBin, sig, payload []byte) error {
	pub, err := decodeECDSAPublicKey(pubBin)
	if err != nil {
		return fmt.Errorf("decode public key: %w", err)
	}
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
	if len(verifScript) == 33 && (verifScript[0] == 0x02 || verifScript[0] == 0x03) {
		return authenticateContainerRequestRFC6979(owner, verifScript, invocScript, payload)
	}

	verifScriptHash := hash.Hash160(verifScript)
	if user.NewFromScriptHash(verifScriptHash) != owner {
		return errOwnerMismatch
	}

	return verifyN3ScriptsNow(fsChain, verifScriptHash, invocScript, verifScript, func() [sha256.Size]byte {
		return sha256.Sum256(payload)
	})
}
