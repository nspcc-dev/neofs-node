package crypto

import (
	"errors"
	"fmt"

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
			return errors.New("owner mismatches signature")
		}
	}
	return nil
}
