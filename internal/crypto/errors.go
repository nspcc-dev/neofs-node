package crypto

import (
	"errors"
	"fmt"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
)

var errIssuerMismatch = errors.New("issuer mismatches signature")

var errMissingSignature = errors.New("missing signature")

var errSignatureMismatch = errors.New("signature mismatch")

// ErrOwnerSignatureMismatch is returned when some owner of the data mismatches
// its signature.
var ErrOwnerSignatureMismatch = errors.New("owner mismatches signature")

func schemeError(s neofscrypto.Scheme, cause error) error {
	return fmt.Errorf("scheme %v: %w", s, cause)
}
