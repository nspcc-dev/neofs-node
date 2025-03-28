package crypto

import (
	"fmt"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
)

func schemeError(s neofscrypto.Scheme, cause error) error {
	return fmt.Errorf("scheme %v: %w", s, cause)
}
