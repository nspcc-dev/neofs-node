package intermediate

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
	eigencalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
	eigentrustctrl "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/controller"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
)

// InitialTrustSource is an implementation of the
// reputation/eigentrust/calculator's InitialTrustSource interface.
type InitialTrustSource struct {
	NetMap netmap.Source
}

var ErrEmptyNetMap = errors.New("empty NepMap")

// InitialTrust returns `initialTrust` as an initial trust value.
func (i InitialTrustSource) InitialTrust(apireputation.PeerID) (reputation.TrustValue, error) {
	epoch, err := i.NetMap.Epoch()
	if err != nil {
		return reputation.TrustZero, fmt.Errorf("failed to get epoch: %w", err)
	}
	epoch = max(1, epoch) // prevent underflow below
	nm, err := i.NetMap.GetNetMapByEpoch(epoch - 1)
	if err != nil {
		return reputation.TrustZero, fmt.Errorf("failed to get NetMap: %w", err)
	}

	nodeCount := reputation.TrustValueFromFloat64(float64(len(nm.Nodes())))
	if nodeCount == 0 {
		return reputation.TrustZero, ErrEmptyNetMap
	}

	return reputation.TrustOne.Div(nodeCount), nil
}

// DaughtersTrustCalculator wraps EigenTrust calculator and implements the
// eigentrust/calculator's DaughtersTrustCalculator interface.
type DaughtersTrustCalculator struct {
	Calculator *eigencalc.Calculator
}

// Calculate converts and passes values to the wrapped calculator.
func (c *DaughtersTrustCalculator) Calculate(ctx eigentrustctrl.IterationContext) {
	calcPrm := eigencalc.CalculatePrm{}
	epochIteration := eigentrust.EpochIteration{}

	epochIteration.SetEpoch(ctx.Epoch())
	epochIteration.SetI(ctx.I())

	calcPrm.SetLast(ctx.Last())
	calcPrm.SetEpochIteration(epochIteration)

	c.Calculator.Calculate(calcPrm)
}
