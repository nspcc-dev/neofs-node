package locodedb

import (
	"fmt"

	locodecolumn "github.com/nspcc-dev/neofs-node/pkg/util/locode/column"
)

// CountryCode represents a country code for
// the storage in the NeoFS location database.
type CountryCode locodecolumn.CountryCode

// CountryCodeFromString parses a string UN/LOCODE country code
// and returns a CountryCode.
func CountryCodeFromString(s string) (*CountryCode, error) {
	cc, err := locodecolumn.CountryCodeFromString(s)
	if err != nil {
		return nil, fmt.Errorf("could not parse country code: %w", err)
	}

	return CountryFromColumn(cc)
}

// CountryFromColumn converts a UN/LOCODE country code to a CountryCode.
func CountryFromColumn(cc *locodecolumn.CountryCode) (*CountryCode, error) {
	return (*CountryCode)(cc), nil
}

func (c *CountryCode) String() string {
	syms := (*locodecolumn.CountryCode)(c).Symbols()
	return string(syms[:])
}
