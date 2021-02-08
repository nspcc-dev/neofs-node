package continentsdb

import (
	"fmt"
	"sync"

	"github.com/paulmach/orb/geojson"
)

// Prm groups the required parameters of the DB's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// Path to polygons of Earth's continents in GeoJSON format.
	//
	// Must not be empty.
	Path string
}

// DB is a descriptor of the Earth's polygons in GeoJSON format.
//
// For correct operation, DB must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// The DB is immediately ready to work through API.
type DB struct {
	path string

	once sync.Once

	features []*geojson.Feature
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func panicOnPrmValue(n string, v interface{}) {
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
	case prm.Path == "":
		panicOnPrmValue("Path", prm.Path)
	}

	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	return &DB{
		path: prm.Path,
	}
}
