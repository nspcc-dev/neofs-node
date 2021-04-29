package eigentrustcalc

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	"github.com/nspcc-dev/neofs-node/pkg/util"
)

// Prm groups the required parameters of the Calculator's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// Alpha parameter from origin EigenTrust algorithm
	// http://ilpubs.stanford.edu:8090/562/1/2002-56.pdf Ch.5.1.
	//
	// Must be in range (0, 1).
	AlphaProvider AlphaProvider

	// Source of initial node trust values
	//
	// Must not be nil.
	InitialTrustSource InitialTrustSource

	DaughterTrustSource DaughterTrustIteratorProvider

	IntermediateValueTarget common.WriterProvider

	FinalResultTarget IntermediateWriterProvider

	WorkerPool util.WorkerPool
}

// Calculator is a processor of a single iteration of EigenTrust algorithm.
//
// For correct operation, the Calculator must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// the Calculator is immediately ready to work through
// API of external control of calculations and data transfer.
type Calculator struct {
	alpha, beta reputation.TrustValue // beta = 1 - alpha

	prm Prm

	opts *options
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func panicOnPrmValue(n string, v interface{}) {
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}

// New creates a new instance of the Calculator.
//
// Panics if at least one value of the parameters is invalid.
//
// The created Calculator does not require additional
// initialization and is completely ready for work.
func New(prm Prm, opts ...Option) *Calculator {
	switch {
	case prm.AlphaProvider == nil:
		panicOnPrmValue("AlphaProvider", prm.AlphaProvider)
	case prm.InitialTrustSource == nil:
		panicOnPrmValue("InitialTrustSource", prm.InitialTrustSource)
	case prm.DaughterTrustSource == nil:
		panicOnPrmValue("DaughterTrustSource", prm.DaughterTrustSource)
	case prm.IntermediateValueTarget == nil:
		panicOnPrmValue("IntermediateValueTarget", prm.IntermediateValueTarget)
	case prm.FinalResultTarget == nil:
		panicOnPrmValue("FinalResultTarget", prm.FinalResultTarget)
	case prm.WorkerPool == nil:
		panicOnPrmValue("WorkerPool", prm.WorkerPool)
	}

	o := defaultOpts()

	for _, opt := range opts {
		opt(o)
	}

	return &Calculator{
		prm:  prm,
		opts: o,
	}
}
