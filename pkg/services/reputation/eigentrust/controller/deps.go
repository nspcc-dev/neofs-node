package eigentrustctrl

import (
	"context"
)

// IterationContext is a context of the i-th
// stage of iterative EigenTrust algorithm.
type IterationContext interface {
	context.Context

	// Must return epoch number to select the values
	// for global trust calculation.
	Epoch() uint64

	// Must return the sequence number of the iteration.
	I() uint32

	// Must return true if I() is the last iteration.
	Last() bool
}

// DaughtersTrustCalculator is an interface of entity
// responsible for calculating the global trust of
// daughter nodes in terms of EigenTrust algorithm.
type DaughtersTrustCalculator interface {
	// Must perform the iteration step of the loop
	// for computing the global trust of all daughter
	// nodes and sending intermediate values
	// according to EigenTrust description
	// http://ilpubs.stanford.edu:8090/562/1/2002-56.pdf Ch.5.1.
	//
	// Execution should be interrupted if ctx.Last().
	Calculate(ctx IterationContext)
}
