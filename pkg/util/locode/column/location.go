package locodecolumn

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
)

const locationCodeLen = 3

// LocationCode represents 3-character code for the location.
type LocationCode [locationCodeLen]uint8

// Symbols returns characters of the location code.
func (lc *LocationCode) Symbols() [locationCodeLen]uint8 {
	return *lc
}

// LocationCodeFromString parses a string and returns the location code.
func LocationCodeFromString(s string) (*LocationCode, error) {
	if l := len(s); l != locationCodeLen {
		return nil, fmt.Errorf("incorrect location code length: expect: %d, got: %d",
			locationCodeLen,
			l,
		)
	}

	for i := range s {
		if !isUpperAlpha(s[i]) && !isDigit(s[i]) {
			return nil, locode.ErrInvalidString
		}
	}

	lc := LocationCode{}
	copy(lc[:], s)

	return &lc, nil
}
