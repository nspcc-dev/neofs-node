package audit

import (
	"bytes"
	"encoding/hex"
	"math/big"

	"github.com/nspcc-dev/neofs-api-go/pkg/audit"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// CalculatePrm groups required parameters of
// Calculator.CalculateForEpoch call.
type CalculatePrm struct {
	// Number of epoch to perform the calculation.
	Epoch uint64
}

type singleResultCtx struct {
	eAudit uint64

	auditResult *audit.Result

	log *logger.Logger

	cid *container.ID

	txTable *transferTable

	cnrInfo ContainerInfo

	cnrNodes []NodeInfo

	passNodes map[string]NodeInfo

	sumSGSize *big.Int
}

var (
	bigGB   = big.NewInt(1 << 30)
	bigZero = big.NewInt(0)
	bigOne  = big.NewInt(1)
)

// Calculate calculates payments for audit results in a specific epoch of the network.
// Wraps the results in a money transfer transaction and sends it to the network.
func (c *Calculator) Calculate(p *CalculatePrm) {
	log := c.opts.log.With(
		zap.Uint64("current epoch", p.Epoch),
	)

	log.Info("calculate audit settlements")

	log.Debug("getting results for the previous epoch")

	auditResults, err := c.prm.ResultStorage.AuditResultsForEpoch(p.Epoch - 1)
	if err != nil {
		log.Error("could not collect audit results")
		return
	} else if len(auditResults) == 0 {
		log.Debug("no audit results in previous epoch")
		return
	}

	log.Debug("processing audit results",
		zap.Int("number", len(auditResults)),
	)

	table := newTransferTable()

	for i := range auditResults {
		c.processResult(&singleResultCtx{
			log:         log,
			auditResult: auditResults[i],
			txTable:     table,
		})
	}

	log.Debug("processing transfers")

	table.iterate(func(tx *transferTx) {
		sign := tx.amount.Sign()
		if sign == 0 {
			log.Debug("ignore zero transfer")
			return
		}

		if sign < 0 {
			tx.from, tx.to = tx.to, tx.from
			tx.amount.Neg(tx.amount)
		}

		c.prm.Exchanger.Transfer(tx.from, tx.to, tx.amount)
	})
}

func (c *Calculator) processResult(ctx *singleResultCtx) {
	ctx.log = ctx.log.With(
		zap.Stringer("cid", ctx.containerID()),
		zap.Uint64("audit epoch", ctx.auditResult.AuditEpoch()),
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
	var err error

	ctx.cnrInfo, err = c.prm.ContainerStorage.ContainerInfo(ctx.auditResult.ContainerID())
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
	ctx.passNodes = make(map[string]NodeInfo)

loop:
	for _, cnrNode := range ctx.cnrNodes {
		for _, passNode := range ctx.auditResult.PassNodes() {
			if !bytes.Equal(cnrNode.PublicKey(), passNode) {
				continue
			}

			for _, failNode := range ctx.auditResult.FailNodes() {
				if bytes.Equal(cnrNode.PublicKey(), failNode) {
					continue loop
				}
			}

			ctx.passNodes[hex.EncodeToString(passNode)] = cnrNode
		}
	}

	empty := len(ctx.passNodes) == 0
	if empty {
		ctx.log.Debug("none of the container nodes passed the audit")
	}

	return !empty
}

func (c *Calculator) sumSGSizes(ctx *singleResultCtx) bool {
	passedSG := ctx.auditResult.PassSG()

	if len(passedSG) == 0 {
		ctx.log.Debug("empty list of passed SG")
		return false
	}

	sumPassSGSize := uint64(0)

	addr := object.NewAddress()
	addr.SetContainerID(ctx.containerID())

	for _, sgID := range ctx.auditResult.PassSG() {
		addr.SetObjectID(sgID)

		sgInfo, err := c.prm.SGStorage.SGInfo(addr)
		if err != nil {
			ctx.log.Error("could not get SG info",
				zap.Stringer("id", sgID),
			)

			return false // we also can continue and calculate at least some part
		}

		sumPassSGSize += sgInfo.Size()
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

		if fee.Cmp(bigZero) == 0 {
			fee.Add(fee, bigOne)
		}

		ctx.txTable.transfer(&transferTx{
			from:   cnrOwner,
			to:     ownerID,
			amount: fee,
		})
	}

	return false
}

func (c *singleResultCtx) containerID() *container.ID {
	if c.cid == nil {
		c.cid = c.auditResult.ContainerID()
	}

	return c.cid
}

func (c *singleResultCtx) auditEpoch() uint64 {
	if c.eAudit == 0 {
		c.eAudit = c.auditResult.AuditEpoch()
	}

	return c.eAudit
}
