package crypto

import (
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// AuthenticateContainerRequest checks whether given payload of the request of
// the container is signed correctly by its owner. Returns
// [ErrOwnerSignatureMismatch] if owner mismatches the signature.
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
			return ErrOwnerSignatureMismatch
		}
	}
	return nil
}
