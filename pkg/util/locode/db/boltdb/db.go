package locodebolt

import (
	"fmt"
	"os"

	"go.etcd.io/bbolt"
)

// Prm groups the required parameters of the DB's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// Path to BoltDB file with NeoFS location database.
	//
	// Must not be empty.
	Path string
}

// DB is a descriptor of the NeoFS BoltDB location database.
//
// For correct operation, DB must be created
// using the constructor (New) based on the required parameters
// and optional components.
//
// After successful creation,
// DB must be opened through Open call. After successful opening,
// DB is ready to work through API (until Close call).
//
// Upon completion of work with the DB, it must be closed
// by Close method.
type DB struct {
	path string

	mode os.FileMode

	boltOpts *bbolt.Options

	bolt *bbolt.DB
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func panicOnPrmValue(n string, v interface{}) {
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}

// New creates a new instance of the DB.
//
// Panics if at least one value of the parameters is invalid.
//
// The created DB requires calling the Open method in order
// to initialize required resources.
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
		path:     prm.Path,
		mode:     o.mode,
		boltOpts: o.boltOpts,
	}
}
