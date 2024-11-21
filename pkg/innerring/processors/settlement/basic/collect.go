package basic

import (
	"math/big"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/common"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"go.uber.org/zap"
)

var (
	bigGB  = big.NewInt(1 << 30)
	bigOne = big.NewInt(1)
)

func (inc *IncomeSettlementContext) Collect() {
	inc.mu.Lock()
	defer inc.mu.Unlock()

	cachedRate, err := inc.rate.BasicRate()
	if err != nil {
		inc.log.Error("can't get basic income rate",
			zap.Error(err))

		return
	}

	cnrEstimations, err := inc.estimations.Estimations(inc.epoch)
	if err != nil {
		inc.log.Error("can't fetch container size estimations",
			zap.Uint64("epoch", inc.epoch),
			zap.Error(err))

		return
	}

	txTable := common.NewTransferTable()

	for cnr, e := range cnrEstimations {
		owner, err := inc.container.ContainerInfo(cnr)
		if err != nil {
			inc.log.Warn("can't fetch container info",
				zap.Uint64("epoch", inc.epoch),
				zap.Stringer("container_id", cnr),
				zap.Error(err))

			continue
		}

		cnrNodes, err := inc.placement.ContainerNodes(inc.epoch, cnr)
		if err != nil {
			inc.log.Debug("can't fetch container info",
				zap.Uint64("epoch", inc.epoch),
				zap.Stringer("container_id", cnr),
				zap.Error(err))

			continue
		}

		avg := inc.avgEstimation(e) // average container size per node
		total := calculateBasicSum(avg, cachedRate, len(cnrNodes))

		// fill distribute asset table
		for i := range cnrNodes {
			inc.distributeTable.Put(cnrNodes[i].PublicKey(), avg)
		}

		txTable.Transfer(&common.TransferTx{
			From:   owner.Owner(),
			To:     inc.bankOwner,
			Amount: total,
		})
	}

	common.TransferAssets(inc.exchange, txTable, common.BasicIncomeCollectionDetails(inc.epoch))
}

// avgEstimation returns estimation value for a single container. Right now it
// simply calculates an average of all announcements, however it can be smarter and
// base the result on reputation of the announcers and clever math.
func (inc *IncomeSettlementContext) avgEstimation(e *cntClient.Estimations) (avg uint64) {
	if len(e.Values) == 0 {
		return 0
	}

	for i := range e.Values {
		avg += e.Values[i].Size
	}

	return avg / uint64(len(e.Values))
}

func calculateBasicSum(size, rate uint64, ln int) *big.Int {
	bigRate := big.NewInt(int64(rate))

	total := size * uint64(ln)

	price := big.NewInt(0).SetUint64(total)
	price.Mul(price, bigRate)
	price.Div(price, bigGB)

	if price.Sign() == 0 {
		price.Add(price, bigOne)
	}

	return price
}
