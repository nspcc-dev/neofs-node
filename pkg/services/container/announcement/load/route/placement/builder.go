package placementrouter

import "fmt"

// Prm groups the required parameters of the Builder's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// Calculator of the container members.
	//
	// Must not be nil.
	PlacementBuilder PlacementBuilder
}

// Builder represents component that routes used container space
// values between nodes from the container.
//
// For correct operation, Builder must be created using
// the constructor (New) based on the required parameters
// and optional components. After successful creation,
// the Builder is immediately ready to work through API.
type Builder struct {
	placementBuilder PlacementBuilder
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
// initialization and is completely ready for work
func New(prm Prm) *Builder {
	switch {
	case prm.PlacementBuilder == nil:
		panicOnPrmValue("RemoteWriterProvider", prm.PlacementBuilder)
	}

	return &Builder{
		placementBuilder: prm.PlacementBuilder,
	}
}
