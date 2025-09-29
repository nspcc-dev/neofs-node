package settlement

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"math/big"
	"slices"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/common"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type payment struct {
	owner  user.ID
	amount *big.Int
}
type incomeReceivers struct {
	m     sync.Mutex
	nodes map[string][]payment
}

func (ir *incomeReceivers) add(nodeKey string, p payment) {
	ir.m.Lock()
	defer ir.m.Unlock()

	if payers, ok := ir.nodes[nodeKey]; ok {
		i := slices.IndexFunc(ir.nodes[nodeKey], func(registeredPayment payment) bool {
			return registeredPayment.owner == p.owner
		})
		if i == -1 {
			payers = append(payers, p)
			ir.nodes[nodeKey] = payers

			return
		}

		payers[i].amount.Add(payers[i].amount, p.amount)

		return
	}

	ir.nodes[nodeKey] = []payment{p}
}

func (p *Processor) HandleBasicIncomeEvent(e event.Event) {
	ev := e.(BasicIncomeEvent)
	epoch := ev.Epoch()
	l := p.log.With(zap.Uint64("epoch", epoch))

	if !p.state.IsAlphabet() {
		l.Info("non alphabet mode, ignore income collection event")

		return
	}

	rate, err := p.nmClient.BasicIncomeRate()
	if err != nil {
		l.Error("can't get basic income rate",
			zap.Error(err))

		return
	}
	if rate == 0 {
		l.Info("basic income rate is zero, skipping collection")
		return
	}

	cnrs, err := p.cnrClient.List(nil)
	if err != nil {
		l.Warn("failed to list containers", zap.Error(err))
		return
	}

	l.Info("start basic income calculation...")

	payments := p.calculatePayments(l, rate, cnrs)

	l.Info("basic income calculated, transfer tokens...", zap.Int("numberOfRecievers", len(payments.nodes)))

	p.distributePayments(l, epoch, payments)

	l.Debug("finished basic income distribution")
}

func (p *Processor) calculatePayments(l *zap.Logger, paymentRate uint64, cnrs []cid.ID) *incomeReceivers {
	var (
		wg            errgroup.Group
		transferTable = incomeReceivers{nodes: make(map[string][]payment)}
	)

	wg.SetLimit(parallelFactor)
	for _, cID := range cnrs {
		wg.Go(func() error {
			cnr, err := p.cnrClient.Get(cID[:])
			if err != nil {
				l.Warn("failed to get container, it will be skipped", zap.Stringer("cID", cID), zap.Error(err))
				return nil
			}
			owner := cnr.Owner()

			reports, err := p.cnrClient.NodeReports(cID)
			if err != nil {
				l.Warn("failed to get container reports, container will be skipped", zap.Stringer("cID", cID), zap.Error(err))
				return nil
			}
			for _, r := range reports {
				switch {
				case r.StorageSize == 0:
					l.Debug("skipping container with zero storage size", zap.Stringer("cID", cID))
					return nil
				case r.StorageSize < 0:
					l.Warn("skipping container with negative storage size",
						zap.Stringer("cID", cID), zap.Int64("storageSize", r.StorageSize))
					return nil
				default:
				}

				transferTable.add(string(r.Reporter), payment{owner: owner, amount: calculateBasicSum(uint64(r.StorageSize), paymentRate)})
			}

			return nil
		})
	}
	_ = wg.Wait() // always nil errors are returned in routines

	return &transferTable
}

func (p *Processor) distributePayments(l *zap.Logger, epoch uint64, payments *incomeReceivers) {
	var (
		wg                  errgroup.Group
		distributionDetails = common.BasicIncomeDistributionDetails(epoch)
	)
	// r/o operations only, no mutex needed to be taken
	for rawKey, payers := range payments.nodes {
		pubKey, err := keys.NewPublicKeyFromBytes([]byte(rawKey), elliptic.P256())
		if err != nil {
			l.Warn("failed to decode public key, skip income receiver",
				zap.Binary("reporter", []byte(rawKey)), zap.Error(err))
			continue
		}
		receiverID := user.NewFromECDSAPublicKey(ecdsa.PublicKey(*pubKey))

		// TODO: batch it?
		for _, payer := range payers {
			wg.Go(func() error {
				l := l.With(
					zap.Stringer("sender", payer.owner),
					zap.Stringer("recipient", receiverID),
					zap.Stringer("amount (GASe-12)", payer.amount),
					zap.Binary("paymentDetails", distributionDetails))

				err := p.balanceClient.TransferX(payer.owner, receiverID, payer.amount, distributionDetails)
				if err != nil {
					l.Error("could not send transfer", zap.Error(err))
					return nil
				}

				l.Debug("successfully sent transfer")

				return nil
			})
		}

		_ = wg.Wait()
	}
}

func calculateBasicSum(size, rate uint64) *big.Int {
	bigRate := big.NewInt(int64(rate))

	price := big.NewInt(int64(size))
	price.Mul(price, bigRate)
	price.Div(price, bigGB)

	if price.Sign() == 0 {
		price.Add(price, bigOne)
	}

	return price
}
