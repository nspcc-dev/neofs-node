package eigentrustctrl

import (
	"fmt"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/util"
)

// Prm groups the required parameters of the Controller's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// Number of iterations
	IterationNumber uint32

	// Component of computing iteration of EigenTrust algorithm.
	//
	// Must not be nil.
	DaughtersTrustCalculator DaughtersTrustCalculator

	// Routine execution pool for single epoch iteration.
	WorkerPool util.WorkerPool
}

// Controller represents EigenTrust algorithm transient controller.
//
// Controller's main goal is to separate the two main stages of
// the calculation:
//  1.reporting local values to manager nodes
//  2.calculating global trusts of child nodes
//
// Calculation stages are controlled based on external signals
// that come from the application through the Controller's API.
//
// For correct operation, the controller must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// the constructor is immediately ready to work through
// API of external control of calculations and data transfer.
type Controller struct {
	prm Prm

	opts *options

	mtx  sync.Mutex
	mCtx map[uint64]*iterContextCancel
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func panicOnPrmValue(n string, v interface{}) {
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}

// New creates a new instance of the Controller.
//
// Panics if at least one value of the parameters is invalid.
//
// The created Controller does not require additional
// initialization and is completely ready for work.
func New(prm Prm, opts ...Option) *Controller {
	switch {
	case prm.IterationNumber == 0:
		panicOnPrmValue("IterationNumber", prm.IterationNumber)
	case prm.WorkerPool == nil:
		panicOnPrmValue("WorkerPool", prm.WorkerPool)
	case prm.DaughtersTrustCalculator == nil:
		panicOnPrmValue("DaughtersTrustCalculator", prm.DaughtersTrustCalculator)
	}

	o := defaultOpts()

	for _, opt := range opts {
		opt(o)
	}

	return &Controller{
		prm:  prm,
		opts: o,
		mCtx: make(map[uint64]*iterContextCancel),
	}
}
