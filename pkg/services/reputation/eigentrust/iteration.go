package eigentrust

import (
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

type IterationTrust struct {
	EpochIteration
	reputation.Trust
}
