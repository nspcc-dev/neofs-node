package eigentrust

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
)

type EpochIteration struct {
	e uint64
	i uint32
}

func (x EpochIteration) Epoch() uint64 {
	return x.e
}

func (x *EpochIteration) SetEpoch(e uint64) {
	x.e = e
}

func (x EpochIteration) I() uint32 {
	return x.i
}

func (x *EpochIteration) SetI(i uint32) {
	x.i = i
}

func (x *EpochIteration) Increment() {
	x.i++
}

type IterationTrust struct {
	EpochIteration
	reputation.Trust
}

// IterContext aggregates context and data required for
// iterations.
type IterContext struct {
	context.Context
	EpochIteration
}

func NewIterContext(ctx context.Context, epoch uint64, iter uint32) *IterContext {
	ei := EpochIteration{}

	ei.SetI(iter)
	ei.SetEpoch(epoch)

	return &IterContext{
		Context:        ctx,
		EpochIteration: ei,
	}
}
