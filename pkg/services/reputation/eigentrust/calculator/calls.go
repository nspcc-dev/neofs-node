package eigentrustcalc

import (
	"context"
	"encoding/hex"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
	"go.uber.org/zap"
)

type CalculatePrm struct {
	last bool

	ei eigentrust.EpochIteration
}

func (p *CalculatePrm) SetLast(last bool) {
	p.last = last
}

func (p *CalculatePrm) SetEpochIteration(ei eigentrust.EpochIteration) {
	p.ei = ei
}

func (c *Calculator) Calculate(prm CalculatePrm) {
	ctx := eigentrust.IterContext{
		Context:        context.Background(),
		EpochIteration: prm.ei,
	}

	iter := ctx.I()

	if iter == 0 {
		c.sendInitialValues(ctx)
		return
	}

	// decrement iteration number to select the values collected
	// on the previous stage
	ctx.SetI(iter - 1)

	consumersIter, err := c.prm.DaughterTrustSource.InitConsumersIterator(ctx)
	if err != nil {
		c.opts.log.Debug("consumers trust iterator's init failure",
			zap.String("error", err.Error()),
		)

		return
	}

	// continue with initial iteration number
	ctx.SetI(iter)

	err = consumersIter.Iterate(func(daughter reputation.PeerID, iter TrustIterator) error {
		err := c.prm.WorkerPool.Submit(func() {
			c.iterateDaughter(iterDaughterPrm{
				lastIter:      prm.last,
				ctx:           ctx,
				id:            daughter,
				consumersIter: iter,
			})
		})
		if err != nil {
			c.opts.log.Debug("worker pool submit failure",
				zap.String("error", err.Error()),
			)
		}

		// don't stop trying
		return nil
	})
	if err != nil {
		c.opts.log.Debug("iterate daughters failed",
			zap.String("error", err.Error()),
		)
	}
}

type iterDaughterPrm struct {
	lastIter bool

	ctx Context

	id reputation.PeerID

	consumersIter TrustIterator
}

func (c *Calculator) iterateDaughter(p iterDaughterPrm) {
	initTrust, err := c.prm.InitialTrustSource.InitialTrust(p.id)
	if err != nil {
		c.opts.log.Debug("get initial trust failure",
			zap.String("daughter", hex.EncodeToString(p.id.Bytes())),
			zap.String("error", err.Error()),
		)

		return
	}

	daughterIter, err := c.prm.DaughterTrustSource.InitDaughterIterator(p.ctx, p.id)
	if err != nil {
		c.opts.log.Debug("daughter trust iterator's init failure",
			zap.String("error", err.Error()),
		)

		return
	}

	sum := reputation.TrustZero

	err = p.consumersIter.Iterate(func(trust reputation.Trust) error {
		if !p.lastIter {
			select {
			case <-p.ctx.Done():
				return p.ctx.Err()
			default:
			}
		}

		sum.Add(trust.Value())
		return nil
	})
	if err != nil {
		c.opts.log.Debug("iterate over daughter's trusts failure",
			zap.String("error", err.Error()),
		)

		return
	}

	// Alpha * Pd
	initTrust.Mul(c.alpha)

	sum.Mul(c.beta)
	sum.Add(initTrust)

	var intermediateTrust eigentrust.IterationTrust

	intermediateTrust.SetEpoch(p.ctx.Epoch())
	intermediateTrust.SetPeer(p.id)
	intermediateTrust.SetI(p.ctx.I())

	if p.lastIter {
		finalWriter, err := c.prm.FinalResultTarget.InitIntermediateWriter(p.ctx)
		if err != nil {
			c.opts.log.Debug("init intermediate writer failure",
				zap.String("error", err.Error()),
			)

			return
		}

		intermediateTrust.SetValue(sum)

		err = finalWriter.WriteIntermediateTrust(intermediateTrust)
		if err != nil {
			c.opts.log.Debug("write final result failure",
				zap.String("error", err.Error()),
			)

			return
		}
	} else {
		intermediateWriter, err := c.prm.IntermediateValueTarget.InitWriter(p.ctx)
		if err != nil {
			c.opts.log.Debug("init intermediate writer failure",
				zap.String("error", err.Error()),
			)

			return
		}

		err = daughterIter.Iterate(func(trust reputation.Trust) error {
			select {
			case <-p.ctx.Done():
				return p.ctx.Err()
			default:
			}

			val := trust.Value()
			val.Mul(sum)

			trust.SetValue(val)

			err := intermediateWriter.Write(p.ctx, trust)
			if err != nil {
				c.opts.log.Debug("write intermediate value failure",
					zap.String("error", err.Error()),
				)
			}

			return nil
		})
		if err != nil {
			c.opts.log.Debug("iterate daughter trusts failure",
				zap.String("error", err.Error()),
			)
		}

		err = intermediateWriter.Close()
		if err != nil {
			c.opts.log.Error(
				"could not close intermediate writer",
				zap.String("error", err.Error()),
			)
		}
	}
}

func (c *Calculator) sendInitialValues(ctx Context) {
	daughterIter, err := c.prm.DaughterTrustSource.InitAllDaughtersIterator(ctx)
	if err != nil {
		c.opts.log.Debug("all daughters trust iterator's init failure",
			zap.String("error", err.Error()),
		)

		return
	}

	intermediateWriter, err := c.prm.IntermediateValueTarget.InitWriter(ctx)
	if err != nil {
		c.opts.log.Debug("init intermediate writer failure",
			zap.String("error", err.Error()),
		)

		return
	}

	err = daughterIter.Iterate(func(daughter reputation.PeerID, iterator TrustIterator) error {
		return iterator.Iterate(func(trust reputation.Trust) error {
			trusted := trust.Peer()

			initTrust, err := c.prm.InitialTrustSource.InitialTrust(trusted)
			if err != nil {
				c.opts.log.Debug("get initial trust failure",
					zap.String("peer", hex.EncodeToString(trusted.Bytes())),
					zap.String("error", err.Error()),
				)

				// don't stop on single failure
				return nil
			}

			initTrust.Mul(trust.Value())
			trust.SetValue(initTrust)

			err = intermediateWriter.Write(ctx, trust)
			if err != nil {
				c.opts.log.Debug("write intermediate value failure",
					zap.String("error", err.Error()),
				)

				// don't stop on single failure
			}

			return nil
		})
	})
	if err != nil {
		c.opts.log.Debug("iterate over all daughters failure",
			zap.String("error", err.Error()),
		)
	}

	err = intermediateWriter.Close()
	if err != nil {
		c.opts.log.Debug("could not close intermediate writer",
			zap.String("error", err.Error()),
		)
	}
}
