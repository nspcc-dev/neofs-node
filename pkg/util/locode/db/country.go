package locodedb

import (
	"fmt"

	locodecolumn "github.com/nspcc-dev/neofs-node/pkg/util/locode/column"
)

// CountryCode represents country code for
// storage in the NeoFS location database.
type CountryCode locodecolumn.CountryCode

// CountryCodeFromString parses string UN/LOCODE country code
// and returns CountryCode.
func CountryCodeFromString(s string) (*CountryCode, error) {
	cc, err := locodecolumn.CountryCodeFromString(s)
	if err != nil {
		return nil, fmt.Errorf("could not parse country code: %w", err)
	}

	return CountryFromColumn(cc)
}

// CountryFromColumn converts UN/LOCODE country code to CountryCode.
func CountryFromColumn(cc *locodecolumn.CountryCode) (*CountryCode, error) {
	return (*CountryCode)(cc), nil
}

func (c *CountryCode) String() string {
	syms := (*locodecolumn.CountryCode)(c).Symbols()
	return string(syms[:])
}
