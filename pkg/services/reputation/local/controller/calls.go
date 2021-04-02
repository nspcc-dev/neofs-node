package trustcontroller

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// ReportPrm groups the required parameters of the Controller.Report method.
type ReportPrm struct {
	epoch uint64
}

// SetEpoch sets epoch number to select reputation values.
func (p *ReportPrm) SetEpoch(e uint64) {
	p.epoch = e
}

// Report reports local reputation values.
//
// Single Report operation overtakes all data from LocalTrustSource
// to LocalTrustTarget (Controller's parameters).
//
// Each call acquires a report context for an Epoch parameter.
// At the very end of the operation, the context is released.
func (c *Controller) Report(prm ReportPrm) {
	// acquire report
	reportCtx := c.acquireReport(prm.epoch)
	if reportCtx == nil {
		return
	}

	// report local trust values
	reportCtx.report()

	// finally stop and free the report
	c.freeReport(prm.epoch, reportCtx.log)
}

type reportContext struct {
	epoch uint64

	ctrl *Controller

	log *logger.Logger

	ctx Context
}

type iteratorContext struct {
	context.Context

	epoch uint64
}

func (c iteratorContext) Epoch() uint64 {
	return c.epoch
}

func (c *Controller) acquireReport(epoch uint64) *reportContext {
	var ctx context.Context

	c.mtx.Lock()

	{
		if cancel := c.mCtx[epoch]; cancel == nil {
			ctx, cancel = context.WithCancel(context.Background())
			c.mCtx[epoch] = cancel
		}
	}

	c.mtx.Unlock()

	log := c.opts.log.With(
		zap.Uint64("epoch", epoch),
	)

	if ctx == nil {
		log.Debug("report is already started")
		return nil
	}

	return &reportContext{
		epoch: epoch,
		ctrl:  c,
		log:   log,
		ctx: &iteratorContext{
			Context: ctx,
			epoch:   epoch,
		},
	}
}

func (c *reportContext) report() {
	c.log.Debug("starting to report local trust values")

	// initialize iterator over locally collected values
	iterator, err := c.ctrl.prm.LocalTrustSource.InitIterator(c.ctx)
	if err != nil {
		c.log.Debug("could not initialize iterator over local trust values",
			zap.String("error", err.Error()),
		)

		return
	}

	// initialize target of local trust values
	targetWriter, err := c.ctrl.prm.LocalTrustTarget.InitWriter(c.ctx)
	if err != nil {
		c.log.Debug("could not initialize local trust target",
			zap.String("error", err.Error()),
		)

		return
	}

	// iterate over all values and write them to the target
	err = iterator.Iterate(
		func(t reputation.Trust) error {
			// check if context is done
			if err := c.ctx.Err(); err != nil {
				return err
			}

			return targetWriter.Write(t)
		},
	)
	if err != nil && !errors.Is(err, context.Canceled) {
		c.log.Debug("iterator over local trust failed",
			zap.String("error", err.Error()),
		)

		return
	}

	// finish writing
	err = targetWriter.Close()
	if err != nil {
		c.log.Debug("could not finish writing local trust values",
			zap.String("error", err.Error()),
		)

		return
	}

	c.log.Debug("reporting successfully finished")
}

func (c *Controller) freeReport(epoch uint64, log *logger.Logger) {
	var stopped bool

	c.mtx.Lock()

	{
		var cancel context.CancelFunc

		cancel, stopped = c.mCtx[epoch]

		if stopped {
			cancel()
			delete(c.mCtx, epoch)
		}
	}

	c.mtx.Unlock()

	if stopped {
		log.Debug("reporting successfully interrupted")
	} else {
		log.Debug("reporting is not started or already interrupted")
	}
}

// StopPrm groups the required parameters of the Controller.Stop method.
type StopPrm struct {
	epoch uint64
}

// SetEpoch sets epoch number the processing of the values of which must be interrupted.
func (p *StopPrm) SetEpoch(e uint64) {
	p.epoch = e
}

// Stop interrupts the processing of local trust values.
//
// Releases acquired report context.
func (c *Controller) Stop(prm StopPrm) {
	c.freeReport(
		prm.epoch,
		c.opts.log.With(zap.Uint64("epoch", prm.epoch)),
	)
}
