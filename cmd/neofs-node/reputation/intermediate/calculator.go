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

// AlphaProvider provides required alpha parameter of eigen trust algorithm.
// TODO: decide if `Alpha` should be dynamically read from global config. #497
type AlphaProvider struct {
	Alpha float64
}

func (ap AlphaProvider) EigenTrustAlpha() (float64, error) {
	return ap.Alpha, nil
}
