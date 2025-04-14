package crypto

import (
	"crypto/sha256"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// AuthenticateContainerRequest checks whether given payload of the request of
// the container is signed correctly by its owner.
func AuthenticateContainerRequest(owner user.ID, pubBin, sig, payload []byte) error {
	switch len(pubBin) {
	default:
		return fmt.Errorf("invalid/unsupported public key length %d", len(pubBin))
	case 33:
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
	}
	return nil
}

// AuthenticateContainerRequestN3 checks N3 witness of the container owner sent
// along with specified request payload against the current FS chain state.
func AuthenticateContainerRequestN3(owner user.ID, invocScript, verifScript, payload []byte, fsChain N3ScriptRunner) error {
	verifScriptHash := hash.Hash160(verifScript)
	if user.NewFromScriptHash(verifScriptHash) != owner {
		return errOwnerMismatch
	}

	return verifyN3ScriptsNow(fsChain, verifScriptHash, invocScript, verifScript, func() [sha256.Size]byte {
		return sha256.Sum256(payload)
	})
}
