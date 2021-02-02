package basic

import (
	"go.uber.org/zap"
)

func (inc *IncomeSettlementContext) Distribute() {
	inc.mu.Lock()
	defer inc.mu.Unlock()

	bankBalance, err := inc.balances.Balance(inc.bankOwner)
	if err != nil {
		inc.log.Error("can't fetch balance of banking account",
			zap.String("error", err.Error()))

		return
	}

	_ = bankBalance
}
