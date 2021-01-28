package innerring

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/audit"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
)

type blockTimerWrapper timers.BlockTimer

func (t *blockTimerWrapper) ResetEpochTimer() error {
	return (*timers.BlockTimer)(t).Reset()
}

type auditSettlementCalculator audit.Calculator

func (s *auditSettlementCalculator) ProcessAuditSettlements(epoch uint64) {
	(*audit.Calculator)(s).Calculate(&audit.CalculatePrm{
		Epoch: epoch,
	})
}
