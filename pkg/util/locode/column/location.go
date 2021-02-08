package locodecolumn

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
)

const locationCodeLen = 3

// LocationCode represents 3-character code for the location.
type LocationCode [locationCodeLen]uint8

// Symbols returns characters of the location code.
func (lc *LocationCode) Symbols() [locationCodeLen]uint8 {
	return *lc
}

// LocationCodeFromString parses string and returns location code.
func LocationCodeFromString(s string) (*LocationCode, error) {
	if len(s) != locationCodeLen {
		return nil, locode.ErrInvalidString
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
