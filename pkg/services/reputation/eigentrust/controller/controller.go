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
	// Component of computing iteration of EigenTrust algorithm.
	//
	// Must not be nil.
	DaughtersTrustCalculator DaughtersTrustCalculator

	// IterationsProvider provides information about numbers
	// of iterations for algorithm.
	IterationsProvider IterationsProvider

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

	// Number of iterations
	iterationNumber uint32

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
	case prm.IterationsProvider == nil:
		panicOnPrmValue("IterationNumber", prm.IterationsProvider)
	case prm.WorkerPool == nil:
		panicOnPrmValue("WorkerPool", prm.WorkerPool)
	case prm.DaughtersTrustCalculator == nil:
		panicOnPrmValue("DaughtersTrustCalculator", prm.DaughtersTrustCalculator)
	}

	iterations, err := prm.IterationsProvider.EigenTrustIterations()
	if err != nil {
		panic(fmt.Errorf("could not init EigenTrust controller: could not get num of iterations: %w", err))
	}

	o := defaultOpts()

	for _, opt := range opts {
		opt(o)
	}

	return &Controller{
		iterationNumber: uint32(iterations),
		prm:             prm,
		opts:            o,
		mCtx:            make(map[uint64]*iterContextCancel),
	}
}
