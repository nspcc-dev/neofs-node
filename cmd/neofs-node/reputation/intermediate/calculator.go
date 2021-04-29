package intermediate

import (
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
	eigencalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
	eigentrustctrl "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/controller"
)

// DaughtersTrustCalculator wraps EigenTrust calculator and implements
// eigentrust/calculator's DaughtersTrustCalculator interface.
type DaughtersTrustCalculator struct {
	Calculator *eigencalc.Calculator
}

// Calculate converts and passes values to wrapped calculator.
func (c *DaughtersTrustCalculator) Calculate(ctx eigentrustctrl.IterationContext) {
	calcPrm := eigencalc.CalculatePrm{}
	epochIteration := eigentrust.EpochIteration{}

	epochIteration.SetEpoch(ctx.Epoch())
	epochIteration.SetI(ctx.I())

	calcPrm.SetLast(ctx.Last())
	calcPrm.SetEpochIteration(epochIteration)

	c.Calculator.Calculate(calcPrm)
}
