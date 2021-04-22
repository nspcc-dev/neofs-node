package routes

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
)

// Prm groups the required parameters of the Builder's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// Manager builder for current node.
	//
	// Must not be nil.
	ManagerBuilder common.ManagerBuilder
}

// Builder represents component that routes node to its managers.
//
// For correct operation, Builder must be created using
// the constructor (New) based on the required parameters
// and optional components. After successful creation,
// the Builder is immediately ready to work through API.
type Builder struct {
	managerBuilder common.ManagerBuilder
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func panicOnPrmValue(n string, v interface{}) {
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}

// New creates a new instance of the Builder.
//
// Panics if at least one value of the parameters is invalid.
//
// The created Builder does not require additional
// initialization and is completely ready for work.
func New(prm Prm) *Builder {
	switch {
	case prm.ManagerBuilder == nil:
		panicOnPrmValue("ManagerBuilder", prm.ManagerBuilder)
	}

	return &Builder{
		managerBuilder: prm.ManagerBuilder,
	}
}
