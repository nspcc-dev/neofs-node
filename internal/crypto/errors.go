package crypto

import (
	"errors"
	"fmt"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
)

var errIssuerMismatch = errors.New("issuer mismatches signature")

func schemeError(s neofscrypto.Scheme, cause error) error {
	return fmt.Errorf("scheme %v: %w", s, cause)
}
