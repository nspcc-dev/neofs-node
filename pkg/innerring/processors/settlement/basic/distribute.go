package basic

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/common"
	"go.uber.org/zap"
)

func (inc *IncomeSettlementContext) Distribute() {
	inc.mu.Lock()
	defer inc.mu.Unlock()

	total := inc.distributeTable.Total()
	if total.Sign() == 0 {
		inc.log.Info("zero total size of all estimated containers, skip distribution of funds")
		return
	}

	var (
		bankBalance *big.Int
		err         error
		expBackoff  = backoff.NewExponentialBackOff()
		txTable     = common.NewTransferTable()
	)

	expBackoff.InitialInterval = time.Second // Most of our networks have 1s blocks, default 0.5 doesn't make much sense.
	err = backoff.RetryNotify(
		func() error {
			bankBalance, err = inc.balances.Balance(inc.bankOwner)
			if err != nil {
				return err
			}

			if bankBalance.Cmp(total) < 0 {
				return fmt.Errorf("bank balance: %s, expected: %s", bankBalance, total)
			}
			return nil
		},
		expBackoff,
		func(err error, d time.Duration) {
			inc.log.Info("waiting for basic income bank", zap.Error(err), zap.Duration("retry-after", d))
		})

	if err != nil {
		inc.log.Warn("failed to get expected bank balance for distribution", zap.Error(err))
		return
	}

	inc.distributeTable.Iterate(func(key []byte, n *big.Int) {
		nodeOwner, err := inc.accounts.ResolveKey(nodeInfoWrapper(key))
		if err != nil {
			inc.log.Warn("can't transform public key to owner id",
				zap.String("public_key", hex.EncodeToString(key)),
				zap.Error(err))

			return
		}

		txTable.Transfer(&common.TransferTx{
			From:   inc.bankOwner,
			To:     *nodeOwner,
			Amount: normalizedValue(n, total, bankBalance),
		})
	})

	common.TransferAssets(inc.exchange, txTable, common.BasicIncomeDistributionDetails(inc.epoch))
}

func normalizedValue(n, total, limit *big.Int) *big.Int {
	if limit.Sign() == 0 {
		return big.NewInt(0)
	}

	n.Mul(n, limit)
	return n.Div(n, total)
}
