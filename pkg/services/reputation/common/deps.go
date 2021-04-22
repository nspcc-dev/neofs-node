package common

import (
	"context"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/common/router"
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
	// Write must not be called after Close.
	Write(Context, reputation.Trust) error

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

// ManagerBuilder defines an interface for providing a list
// of Managers for specific epoch. Implementation depends on trust value.
type ManagerBuilder interface {
	// BuildManagers must compose list of managers. It depends on
	// particular epoch and PeerID of the current route point.
	BuildManagers(epoch uint64, p reputation.PeerID) ([]router.ServerInfo, error)
}
