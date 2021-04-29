package eigentrustcalc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
)

type Context interface {
	context.Context

	// Must return epoch number to select the values
	// for global trust calculation.
	Epoch() uint64

	// Must return the sequence number of the iteration.
	I() uint32
}

type InitialTrustSource interface {
	InitialTrust(reputation.PeerID) (reputation.TrustValue, error)
}

type TrustIterator interface {
	Iterate(reputation.TrustHandler) error
}

type PeerTrustsHandler func(reputation.PeerID, TrustIterator) error

// PeerTrustsIterator must iterate over all nodes(PeerIDs) and provide
// TrustIterator for iteration over node's Trusts to others peers.
type PeerTrustsIterator interface {
	Iterate(PeerTrustsHandler) error
}

type DaughterTrustIteratorProvider interface {
	// InitDaughterIterator must init TrustIterator
	// that iterates over received local trusts from
	// daughter p for ctx.Epoch() epoch.
	InitDaughterIterator(ctx Context, p reputation.PeerID) (TrustIterator, error)
	// InitAllDaughtersIterator must init PeerTrustsIterator
	// that must iterate over all daughters of the current
	// node(manager) and all trusts received from them for
	// ctx.Epoch() epoch.
	InitAllDaughtersIterator(ctx Context) (PeerTrustsIterator, error)
	// InitConsumersIterator must init PeerTrustsIterator
	// that must iterate over all daughters of the current
	// node(manager) and their consumers' trusts received
	// from other managers for ctx.Epoch() epoch and
	// ctx.I() iteration.
	InitConsumersIterator(Context) (PeerTrustsIterator, error)
}

type IntermediateWriter interface {
	WriteIntermediateTrust(eigentrust.IterationTrust) error
}

type IntermediateWriterProvider interface {
	InitIntermediateWriter(Context) (IntermediateWriter, error)
}
