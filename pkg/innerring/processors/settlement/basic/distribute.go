package basic

import (
	"encoding/hex"
	"math/big"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/common"
	"go.uber.org/zap"
)

func (inc *IncomeSettlementContext) Distribute() {
	inc.mu.Lock()
	defer inc.mu.Unlock()

	txTable := common.NewTransferTable()

	bankBalance, err := inc.balances.Balance(inc.bankOwner)
	if err != nil {
		inc.log.Error("can't fetch balance of banking account",
			zap.String("error", err.Error()))

		return
	}

	total := inc.distributeTable.Total()

	inc.distributeTable.Iterate(func(key []byte, n *big.Int) {
		nodeOwner, err := inc.accounts.ResolveKey(nodeInfoWrapper(key))
		if err != nil {
			inc.log.Warn("can't transform public key to owner id",
				zap.String("public_key", hex.EncodeToString(key)),
				zap.String("error", err.Error()))

			return
		}

		txTable.Transfer(&common.TransferTx{
			From:   inc.bankOwner,
			To:     nodeOwner,
			Amount: normalizedValue(n, total, bankBalance),
		})
	})

	common.TransferAssets(inc.exchange, txTable, common.BasicIncomeDistributionDetails(inc.epoch))
}

func normalizedValue(n, total, limit *big.Int) *big.Int {
	if limit.Cmp(bigZero) == 0 {
		return big.NewInt(0)
	}

	n.Mul(n, limit)
	return n.Div(n, total)
}
