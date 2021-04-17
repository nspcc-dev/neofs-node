package trustcontroller

import (
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
)

// Iterator is a group of methods provided by entity
// which can iterate over a group of reputation.Trust values.
type Iterator interface {
	// Iterate must start an iterator over all trust values.
	// For each value should call a handler, the error
	// of which should be directly returned from the method.
	//
	// Internal failures of the iterator are also signaled via
	// an error. After a successful call to the last value
	// handler, nil should be returned.
	Iterate(reputation.TrustHandler) error
}

// IteratorProvider is a group of methods provided
// by entity which generates iterators over
// reputation.Trust values.
type IteratorProvider interface {
	// InitIterator should return an initialized Iterator
	// that iterates over values from IteratorContext.Epoch() epoch.
	//
	// Initialization problems are reported via error.
	// If no error was returned, then the Iterator must not be nil.
	//
	// Implementations can have different logic for different
	// contexts, so specific ones may document their own behavior.
	InitIterator(common.Context) (Iterator, error)
}
