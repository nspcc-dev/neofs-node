package crypto

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// AuthenticateContainerRequest checks whether given payload of the request of
// the container is signed correctly by its owner.
func AuthenticateContainerRequest(owner user.ID, pubBin, sig, payload []byte, fsChain FSChain) error {
	var signer user.ID
	if len(pubBin) == 33 {
		pub, err := decodeECDSAPublicKey(pubBin)
		if err != nil {
			return fmt.Errorf("decode public key: %w", err)
		}
		if !verifyECDSARFC6979Signature(*pub, sig, payload) {
			return errSignatureMismatch
		}
		signer = user.NewFromECDSAPublicKey(*pub)
	} else { // TODO: can verification script be 33B len?
		if err := verifyN3ScriptsNow(fsChain, sig, pubBin, func() [sha256.Size]byte {
			return sha256.Sum256(payload)
		}); err != nil {
			return err
		}
		signer = user.NewFromScriptHash(hash.Hash160(pubBin)) // TODO: or check signer before?
	}
	if signer != owner {
		return errors.New("owner mismatches signature")
	}
	return nil
}
