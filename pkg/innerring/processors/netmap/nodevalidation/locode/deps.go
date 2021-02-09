package locode

import (
	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
)

// Record is an interface of read-only
// NeoFS LOCODE database single entry.
type Record interface {
	// Must return ISO 3166-1 alpha-2
	// country code.
	//
	// Must not return nil.
	CountryCode() *locodedb.CountryCode

	// Must return English short country name
	// officially used by the ISO 3166
	// Maintenance Agency (ISO 3166/MA).
	CountryName() string

	// Must return UN/LOCODE 3-character code
	// for the location (numerals 2-9 may also
	// be used).
	//
	// Must not return nil.
	LocationCode() *locodedb.LocationCode

	// Must return name of the location which
	// have been allocated a UN/LOCODE without
	// diacritic sign.
	LocationName() string

	// Must return ISO 1-3 character alphabetic
	// and/or numeric code for the administrative
	// division of the country concerned.
	SubDivCode() string

	// Must return subdivision name.
	SubDivName() string

	// Must return existing continent where is
	// the location.
	//
	// Must not return nil.
	Continent() *locodedb.Continent
}

// DB is an interface of read-only
// NeoFS LOCODE database.
type DB interface {
	// Must find the record that corresponds to
	// LOCODE and provides the Record interface.
	//
	// Must return an error if Record is nil.
	//
	// LOCODE is always non-nil.
	Get(*locode.LOCODE) (Record, error)
}
