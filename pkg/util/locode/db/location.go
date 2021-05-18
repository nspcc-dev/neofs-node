package locodedb

import (
	"fmt"

	locodecolumn "github.com/nspcc-dev/neofs-node/pkg/util/locode/column"
)

// LocationCode represents location code for
// storage in the NeoFS location database.
type LocationCode locodecolumn.LocationCode

// LocationCodeFromString parses string UN/LOCODE location code
// and returns LocationCode.
func LocationCodeFromString(s string) (*LocationCode, error) {
	lc, err := locodecolumn.LocationCodeFromString(s)
	if err != nil {
		return nil, fmt.Errorf("could not parse location code: %w", err)
	}

	return LocationFromColumn(lc)
}

// LocationFromColumn converts UN/LOCODE country code to LocationCode.
func LocationFromColumn(cc *locodecolumn.LocationCode) (*LocationCode, error) {
	return (*LocationCode)(cc), nil
}

func (l *LocationCode) String() string {
	syms := (*locodecolumn.LocationCode)(l).Symbols()
	return string(syms[:])
}
