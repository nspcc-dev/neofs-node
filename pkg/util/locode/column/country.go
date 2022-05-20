package locodecolumn

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
)

const countryCodeLen = 2

// CountryCode represents ISO 3166 alpha-2 Country Code.
type CountryCode [countryCodeLen]uint8

// Symbols returns digits of the country code.
func (cc *CountryCode) Symbols() [countryCodeLen]uint8 {
	return *cc
}

// CountryCodeFromString parses a string and returns the country code.
func CountryCodeFromString(s string) (*CountryCode, error) {
	if l := len(s); l != countryCodeLen {
		return nil, fmt.Errorf("incorrect country code length: expect: %d, got: %d",
			countryCodeLen,
			l,
		)
	}

	for i := range s {
		if !isUpperAlpha(s[i]) {
			return nil, locode.ErrInvalidString
		}
	}

	cc := CountryCode{}
	copy(cc[:], s)

	return &cc, nil
}
