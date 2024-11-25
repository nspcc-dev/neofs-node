package eigentrustcalc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
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
	alpha, err := c.prm.AlphaProvider.EigenTrustAlpha()
	if err != nil {
		c.opts.log.Debug(
			"failed to get alpha param",
			zap.Error(err),
		)
		return
	}

	c.alpha = reputation.TrustValueFromFloat64(alpha)
	c.beta = reputation.TrustValueFromFloat64(1 - alpha)

	ctx := eigentrust.IterContext{
		Context:        context.Background(),
		EpochIteration: prm.ei,
	}

	iter := ctx.I()

	log := c.opts.log.With(
		zap.Uint64("epoch", ctx.Epoch()),
		zap.Uint32("iteration", iter),
	)

	if iter == 0 {
		c.sendInitialValues(ctx)
		return
	}

	// decrement iteration number to select the values collected
	// on the previous stage
	ctx.SetI(iter - 1)

	consumersIter, err := c.prm.DaughterTrustSource.InitConsumersIterator(ctx)
	if err != nil {
		log.Debug("consumers trust iterator's init failure",
			zap.Error(err),
		)

		return
	}

	// continue with initial iteration number
	ctx.SetI(iter)

	err = consumersIter.Iterate(func(daughter apireputation.PeerID, iter TrustIterator) error {
		err := c.prm.WorkerPool.Submit(func() {
			c.iterateDaughter(iterDaughterPrm{
				lastIter:      prm.last,
				ctx:           ctx,
				id:            daughter,
				consumersIter: iter,
			})
		})
		if err != nil {
			log.Debug("worker pool submit failure",
				zap.Error(err),
			)
		}

		// don't stop trying
		return nil
	})
	if err != nil {
		log.Debug("iterate daughter's consumers failed",
			zap.Error(err),
		)
	}
}

type iterDaughterPrm struct {
	lastIter bool

	ctx Context

	id apireputation.PeerID

	consumersIter TrustIterator
}

func (c *Calculator) iterateDaughter(p iterDaughterPrm) {
	initTrust, err := c.prm.InitialTrustSource.InitialTrust(p.id)
	if err != nil {
		c.opts.log.Debug("get initial trust failure",
			zap.Stringer("daughter", p.id),
			zap.Error(err),
		)

		return
	}

	daughterIter, err := c.prm.DaughterTrustSource.InitDaughterIterator(p.ctx, p.id)
	if err != nil {
		c.opts.log.Debug("daughter trust iterator's init failure",
			zap.Error(err),
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
			zap.Error(err),
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
			c.opts.log.Debug("init writer failure",
				zap.Error(err),
			)

			return
		}

		intermediateTrust.SetValue(sum)

		err = finalWriter.WriteIntermediateTrust(intermediateTrust)
		if err != nil {
			c.opts.log.Debug("write final result failure",
				zap.Error(err),
			)

			return
		}
	} else {
		intermediateWriter, err := c.prm.IntermediateValueTarget.InitWriter(p.ctx)
		if err != nil {
			c.opts.log.Debug("init writer failure",
				zap.Error(err),
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

			err := intermediateWriter.Write(trust)
			if err != nil {
				c.opts.log.Debug("write value failure",
					zap.Error(err),
				)
			}

			return nil
		})
		if err != nil {
			c.opts.log.Debug("iterate daughter trusts failure",
				zap.Error(err),
			)
		}

		err = intermediateWriter.Close()
		if err != nil {
			c.opts.log.Error(
				"could not close writer",
				zap.Error(err),
			)
		}
	}
}

func (c *Calculator) sendInitialValues(ctx Context) {
	daughterIter, err := c.prm.DaughterTrustSource.InitAllDaughtersIterator(ctx)
	if err != nil {
		c.opts.log.Debug("all daughters trust iterator's init failure",
			zap.Error(err),
		)

		return
	}

	intermediateWriter, err := c.prm.IntermediateValueTarget.InitWriter(ctx)
	if err != nil {
		c.opts.log.Debug("init writer failure",
			zap.Error(err),
		)

		return
	}

	err = daughterIter.Iterate(func(daughter apireputation.PeerID, iterator TrustIterator) error {
		return iterator.Iterate(func(trust reputation.Trust) error {
			trusted := trust.Peer()

			initTrust, err := c.prm.InitialTrustSource.InitialTrust(trusted)
			if err != nil {
				c.opts.log.Debug("get initial trust failure",
					zap.Stringer("peer", trusted),
					zap.Error(err),
				)

				// don't stop on single failure
				return nil
			}

			initTrust.Mul(trust.Value())
			trust.SetValue(initTrust)

			err = intermediateWriter.Write(trust)
			if err != nil {
				c.opts.log.Debug("write value failure",
					zap.Error(err),
				)

				// don't stop on single failure
			}

			return nil
		})
	})
	if err != nil {
		c.opts.log.Debug("iterate over all daughters failure",
			zap.Error(err),
		)
	}

	err = intermediateWriter.Close()
	if err != nil {
		c.opts.log.Debug("could not close writer",
			zap.Error(err),
		)
	}
}
