package locodedb

import (
	locodecolumn "github.com/nspcc-dev/neofs-node/pkg/util/locode/column"
	"github.com/pkg/errors"
)

// LocationCode represents location code for
// storage in the NeoFS location database.
type LocationCode locodecolumn.LocationCode

// LocationCodeFromString parses string UN/LOCODE location code
// and returns LocationCode.
func LocationCodeFromString(s string) (*LocationCode, error) {
	lc, err := locodecolumn.LocationCodeFromString(s)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse location code")
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
