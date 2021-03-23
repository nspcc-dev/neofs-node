package trustcontroller

import (
	"context"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
)

// Context wraps stdlib context
// with accompanying meta values.
type Context interface {
	context.Context

	// Must return epoch number to select the values.
	Epoch() uint64
}

// Writer describes the interface for storing reputation.Trust values.
//
// This interface is provided by both local storage
// of values and remote (wrappers over the RPC).
type Writer interface {
	// Write performs a write operation of reputation.Trust value
	// and returns any error encountered.
	//
	// All values after the Close call must be flushed to the
	// physical target. Implementations can cache values before
	// Close operation.
	//
	// Put must not be called after Close.
	Write(reputation.Trust) error

	// Close exits with method-providing Writer.
	//
	// All cached values must be flushed before
	// the Close's return.
	//
	// Methods must not be called after Close.
	io.Closer
}

// WriterProvider is a group of methods provided
// by entity which generates keepers of
// reputation.Trust values.
type WriterProvider interface {
	// InitWriter should return an initialized Writer.
	//
	// Initialization problems are reported via error.
	// If no error was returned, then the Writer must not be nil.
	//
	// Implementations can have different logic for different
	// contexts, so specific ones may document their own behavior.
	InitWriter(Context) (Writer, error)
}

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
	InitIterator(Context) (Iterator, error)
}
