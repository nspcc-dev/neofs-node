package airportsdb

import (
	"fmt"
	"io/fs"
	"sync"
)

// Prm groups the required parameters of the DB's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// Path to OpenFlights Airport csv table.
	//
	// Must not be empty.
	AirportsPath string

	// Path to OpenFlights Countries csv table.
	//
	// Must not be empty.
	CountriesPath string
}

// DB is a descriptor of the OpenFlights database in csv format.
//
// For correct operation, DB must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// The DB is immediately ready to work through API.
type DB struct {
	airports, countries pathMode

	airportsOnce, countriesOnce sync.Once

	mCountries map[string]string

	mAirports map[string][]record
}

type pathMode struct {
	path string
	mode fs.FileMode
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func panicOnPrmValue(n string, v any) {
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}

// New creates a new instance of the DB.
//
// Panics if at least one value of the parameters is invalid.
//
// The created DB does not require additional
// initialization and is completely ready for work.
func New(prm Prm, opts ...Option) *DB {
	switch {
	case prm.AirportsPath == "":
		panicOnPrmValue("AirportsPath", prm.AirportsPath)
	case prm.CountriesPath == "":
		panicOnPrmValue("CountriesPath", prm.CountriesPath)
	}

	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	return &DB{
		airports: pathMode{
			path: prm.AirportsPath,
			mode: o.airportMode,
		},
		countries: pathMode{
			path: prm.CountriesPath,
			mode: o.countryMode,
		},
	}
}
