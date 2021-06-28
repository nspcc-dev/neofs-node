package csvlocode

import (
	"fmt"
	"io/fs"
	"sync"
)

// Prm groups the required parameters of the Table's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// Path to UN/LOCODE csv table.
	//
	// Must not be empty.
	Path string

	// Path to csv table of UN/LOCODE Subdivisions.
	//
	// Must not be empty.
	SubDivPath string
}

// Table is a descriptor of the UN/LOCODE table in csv format.
//
// For correct operation, Table must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// The Table is immediately ready to work through API.
type Table struct {
	paths []string

	mode fs.FileMode

	subDivPath string

	subDivOnce sync.Once

	mSubDiv map[subDivKey]subDivRecord
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func panicOnPrmValue(n string, v interface{}) {
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}

// New creates a new instance of the Table.
//
// Panics if at least one value of the parameters is invalid.
//
// The created Table does not require additional
// initialization and is completely ready for work.
func New(prm Prm, opts ...Option) *Table {
	switch {
	case prm.Path == "":
		panicOnPrmValue("Path", prm.Path)
	case prm.SubDivPath == "":
		panicOnPrmValue("SubDivPath", prm.SubDivPath)
	}

	o := defaultOpts()

	for i := range opts {
		opts[i](o)
	}

	return &Table{
		paths:      append(o.extraPaths, prm.Path),
		mode:       o.mode,
		subDivPath: prm.SubDivPath,
	}
}
