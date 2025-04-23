package crypto

import (
	"errors"
	"fmt"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
)

var errIssuerMismatch = errors.New("issuer mismatches signature")

var errOwnerMismatch = errors.New("owner mismatches signature")

var errMissingSignature = errors.New("missing signature")

var errSignatureMismatch = errors.New("signature mismatch")

func schemeError(s neofscrypto.Scheme, cause error) error {
	return fmt.Errorf("scheme %v: %w", s, cause)
}

// ErrUnsupportedScheme is returned when [neofscrypto.N3] signature cannot be
// verified without FS chain state.
type ErrUnsupportedScheme neofscrypto.Scheme

func (x ErrUnsupportedScheme) Error() string {
	return fmt.Sprintf("unsupported scheme %v", neofscrypto.Scheme(x))
}
