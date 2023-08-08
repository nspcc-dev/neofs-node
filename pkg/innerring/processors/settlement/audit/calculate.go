package audit

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/hex"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/common"
	"github.com/nspcc-dev/neofs-sdk-go/audit"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

// CalculatePrm groups the required parameters of
// Calculator.CalculateForEpoch call.
type CalculatePrm struct {
	// Number of epoch to perform the calculation.
	Epoch uint64
}

type singleResultCtx struct {
	eAudit uint64

	auditResult *audit.Result

	log *zap.Logger

	txTable *common.TransferTable

	cnrInfo common.ContainerInfo

	cnrNodes []common.NodeInfo

	passNodes map[string]common.NodeInfo

	sumSGSize *big.Int

	auditFee *big.Int
}

var (
	bigGB  = big.NewInt(1 << 30)
	bigOne = big.NewInt(1)
)

// Calculate calculates payments for audit results in a specific epoch of the network.
// Wraps the results in a money transfer transaction and sends it to the network.
func (c *Calculator) Calculate(p *CalculatePrm) {
	log := c.opts.log.With(
		zap.Uint64("current epoch", p.Epoch),
	)

	if p.Epoch == 0 {
		log.Info("settlements are ignored for zero epoch")
		return
	}

	log.Info("calculate audit settlements")

	log.Debug("getting results for the previous epoch")
	prevEpoch := p.Epoch - 1

	auditResults, err := c.prm.ResultStorage.AuditResultsForEpoch(prevEpoch)
	if err != nil {
		log.Error("could not collect audit results")
		return
	} else if len(auditResults) == 0 {
		log.Debug("no audit results in previous epoch")
		return
	}

	auditFee, err := c.prm.AuditFeeFetcher.AuditFee()
	if err != nil {
		log.Warn("can't fetch audit fee from network config",
			zap.String("error", err.Error()))
		auditFee = 0
	}

	log.Debug("processing audit results",
		zap.Int("number", len(auditResults)),
	)

	table := common.NewTransferTable()

	for i := range auditResults {
		c.processResult(&singleResultCtx{
			log:         log,
			auditResult: auditResults[i],
			txTable:     table,
			auditFee:    big.NewInt(0).SetUint64(auditFee),
		})
	}

	log.Debug("processing transfers")

	common.TransferAssets(c.prm.Exchanger, table, common.AuditSettlementDetails(prevEpoch))
}

func (c *Calculator) processResult(ctx *singleResultCtx) {
	ctx.log = ctx.log.With(
		zap.Stringer("cid", ctx.containerID()),
		zap.Uint64("audit epoch", ctx.auditResult.Epoch()),
	)

	ctx.log.Debug("reading information about the container")

	ok := c.readContainerInfo(ctx)
	if !ok {
		return
	}

	ctx.log.Debug("building placement")

	ok = c.buildPlacement(ctx)
	if !ok {
		return
	}

	ctx.log.Debug("collecting passed nodes")

	ok = c.collectPassNodes(ctx)
	if !ok {
		return
	}

	ctx.log.Debug("calculating sum of the sizes of all storage groups")

	ok = c.sumSGSizes(ctx)
	if !ok {
		return
	}

	ctx.log.Debug("filling transfer table")

	c.fillTransferTable(ctx)
}

func (c *Calculator) readContainerInfo(ctx *singleResultCtx) bool {
	cnr, ok := ctx.auditResult.Container()
	if !ok {
		ctx.log.Error("missing container in audit result")
		return false
	}

	var err error

	ctx.cnrInfo, err = c.prm.ContainerStorage.ContainerInfo(cnr)
	if err != nil {
		ctx.log.Error("could not get container info",
			zap.String("error", err.Error()),
		)
	}

	return err == nil
}

func (c *Calculator) buildPlacement(ctx *singleResultCtx) bool {
	var err error

	ctx.cnrNodes, err = c.prm.PlacementCalculator.ContainerNodes(ctx.auditEpoch(), ctx.containerID())
	if err != nil {
		ctx.log.Error("could not get container nodes",
			zap.String("error", err.Error()),
		)
	}

	empty := len(ctx.cnrNodes) == 0
	if empty {
		ctx.log.Debug("empty list of container nodes")
	}

	return err == nil && !empty
}

func (c *Calculator) collectPassNodes(ctx *singleResultCtx) bool {
	ctx.passNodes = make(map[string]common.NodeInfo)

	for _, cnrNode := range ctx.cnrNodes {
		// TODO(@cthulhu-rider): neofs-sdk-go#241 use dedicated method
		ctx.auditResult.IteratePassedStorageNodes(func(passNode []byte) bool {
			if !bytes.Equal(cnrNode.PublicKey(), passNode) {
				return true
			}

			failed := false

			ctx.auditResult.IterateFailedStorageNodes(func(failNode []byte) bool {
				failed = bytes.Equal(cnrNode.PublicKey(), failNode)
				return !failed
			})

			if !failed {
				ctx.passNodes[hex.EncodeToString(passNode)] = cnrNode
			}

			return false
		})
	}

	empty := len(ctx.passNodes) == 0
	if empty {
		ctx.log.Debug("none of the container nodes passed the audit")
	}

	return !empty
}

func (c *Calculator) sumSGSizes(ctx *singleResultCtx) bool {
	sumPassSGSize := uint64(0)
	fail := false

	var addr oid.Address
	addr.SetContainer(ctx.containerID())

	ctx.auditResult.IteratePassedStorageGroups(func(id oid.ID) bool {
		addr.SetObject(id)

		sgInfo, err := c.prm.SGStorage.SGInfo(addr)
		if err != nil {
			ctx.log.Error("could not get SG info",
				zap.String("id", id.String()),
				zap.String("error", err.Error()),
			)

			fail = true

			return false // we also can continue and calculate at least some part
		}

		sumPassSGSize += sgInfo.Size()

		return true
	})

	if fail {
		return false
	}

	if sumPassSGSize == 0 {
		ctx.log.Debug("zero sum SG size")
		return false
	}

	ctx.sumSGSize = big.NewInt(int64(sumPassSGSize))

	return true
}

func (c *Calculator) fillTransferTable(ctx *singleResultCtx) bool {
	cnrOwner := ctx.cnrInfo.Owner()

	// add txs to pay for storage node
	for k, info := range ctx.passNodes {
		ownerID, err := c.prm.AccountStorage.ResolveKey(info)
		if err != nil {
			ctx.log.Error("could not resolve public key of the storage node",
				zap.String("error", err.Error()),
				zap.String("key", k),
			)

			return false // we also can continue and calculate at least some part
		}

		price := info.Price()

		ctx.log.Debug("calculating storage node salary for audit (GASe-12)",
			zap.Stringer("sum SG size", ctx.sumSGSize),
			zap.Stringer("price", price),
		)

		fee := big.NewInt(0).Mul(price, ctx.sumSGSize)
		fee.Div(fee, bigGB)

		if fee.Sign() == 0 {
			fee.Add(fee, bigOne)
		}

		ctx.txTable.Transfer(&common.TransferTx{
			From:   cnrOwner,
			To:     *ownerID,
			Amount: fee,
		})
	}

	// add txs to pay inner ring node for audit result
	auditorKey, err := keys.NewPublicKeyFromBytes(ctx.auditResult.AuditorKey(), elliptic.P256())
	if err != nil {
		ctx.log.Error("could not parse public key of the inner ring node",
			zap.String("error", err.Error()),
			zap.String("key", hex.EncodeToString(ctx.auditResult.AuditorKey())),
		)

		return false
	}

	transferTx := &common.TransferTx{
		From:   cnrOwner,
		Amount: ctx.auditFee,
	}
	transferTx.To = user.ResolveFromECDSAPublicKey(ecdsa.PublicKey(*auditorKey))

	ctx.txTable.Transfer(transferTx)

	return false
}

func (c *singleResultCtx) containerID() cid.ID {
	cnr, _ := c.auditResult.Container()
	return cnr
}

func (c *singleResultCtx) auditEpoch() uint64 {
	if c.eAudit == 0 {
		c.eAudit = c.auditResult.Epoch()
	}

	return c.eAudit
}
