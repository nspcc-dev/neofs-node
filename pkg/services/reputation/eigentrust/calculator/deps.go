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

type PeerTrustsIterator interface {
	Iterate(PeerTrustsHandler) error
}

type DaughterTrustIteratorProvider interface {
	InitDaughterIterator(Context, reputation.PeerID) (TrustIterator, error)
	InitAllDaughtersIterator(Context) (PeerTrustsIterator, error)
	InitConsumersIterator(Context) (PeerTrustsIterator, error)
}

type IntermediateWriter interface {
	WriteIntermediateTrust(eigentrust.IterationTrust) error
}

type IntermediateWriterProvider interface {
	InitIntermediateWriter(Context) (IntermediateWriter, error)
}
