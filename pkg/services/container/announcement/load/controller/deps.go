package loadcontroller

import (
	"context"
	"io"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
)

// UsedSpaceHandler describes the signature of the UsedSpaceAnnouncement
// value handling function.
//
// Termination of processing without failures is usually signaled
// with a zero error, while a specific value may describe the reason
// for failure.
type UsedSpaceHandler func(container.UsedSpaceAnnouncement) error

// UsedSpaceFilter describes the signature of the function for
// checking whether a value meets a certain criterion.
//
// Return of true means conformity, false - vice versa.
type UsedSpaceFilter func(container.UsedSpaceAnnouncement) bool

// Iterator is a group of methods provided by entity
// which can iterate over a group of UsedSpaceAnnouncement values.
type Iterator interface {
	// Iterate must start an iterator over values that
	// meet the filter criterion (returns true).
	// For each such value should call a handler, the error
	// of which should be directly returned from the method.
	//
	// Internal failures of the iterator are also signaled via
	// an error. After a successful call to the last value
	// handler, nil should be returned.
	Iterate(UsedSpaceFilter, UsedSpaceHandler) error
}

// IteratorProvider is a group of methods provided
// by entity which generates iterators over
// UsedSpaceAnnouncement values.
type IteratorProvider interface {
	// InitIterator should return an initialized Iterator.
	//
	// Initialization problems are reported via error.
	// If no error was returned, then the Iterator must not be nil.
	//
	// Implementations can have different logic for different
	// contexts, so specific ones may document their own behavior.
	InitIterator(context.Context) (Iterator, error)
}

// Writer describes the interface for storing UsedSpaceAnnouncement values.
//
// This interface is provided by both local storage
// of values and remote (wrappers over the RPC).
type Writer interface {
	// Put performs a write operation of UsedSpaceAnnouncement value
	// and returns any error encountered.
	//
	// All values after the Close call must be flushed to the
	// physical target. Implementations can cache values before
	// Close operation.
	//
	// Put must not be called after Close.
	Put(container.UsedSpaceAnnouncement) error

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
// UsedSpaceAnnouncement values.
type WriterProvider interface {
	// InitWriter should return an initialized Writer.
	//
	// Initialization problems are reported via error.
	// If no error was returned, then the Writer must not be nil.
	//
	// Implementations can have different logic for different
	// contexts, so specific ones may document their own behavior.
	InitWriter(context.Context) (Writer, error)
}
