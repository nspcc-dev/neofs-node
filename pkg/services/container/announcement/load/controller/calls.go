package loadcontroller

import (
	"context"

	"github.com/nspcc-dev/neofs-sdk-go/container"
	"go.uber.org/zap"
)

// StartPrm groups the required parameters of the Controller.Start method.
type StartPrm struct {
	// Epoch number by which you want to select
	// the values of the used space of containers.
	Epoch uint64
}

type commonContext struct {
	epoch uint64

	ctrl *Controller

	log *zap.Logger

	ctx context.Context
}

type announceContext struct {
	commonContext
}

// Start starts the processing of container.SizeEstimation values.
//
// Single Start operation overtakes all data from LocalMetrics to
// LocalAnnouncementTarget (Controller's parameters).
// No filter by epoch is used for the iterator, since it is expected
// that the source of metrics does not track the change of epochs.
//
// Each call acquires an announcement context for an Epoch parameter.
// At the very end of the operation, the context is released.
func (c *Controller) Start(prm StartPrm) {
	// acquire announcement
	execCtx := c.acquireAnnouncement(prm)
	if execCtx == nil {
		return
	}

	// finally stop and free the announcement
	defer execCtx.freeAnnouncement()

	// announce local values
	execCtx.announce()
}

func (c *announceContext) announce() {
	c.log.Debug("starting to announce local metrics")

	var (
		metricsIterator Iterator
		err             error
	)

	// initialize iterator over locally collected metrics
	metricsIterator, err = c.ctrl.prm.LocalMetrics.InitIterator(c.ctx)
	if err != nil {
		c.log.Debug("could not initialize iterator over locally collected metrics",
			zap.Error(err),
		)

		return
	}

	// initialize target of local announcements
	targetWriter, err := c.ctrl.prm.LocalAnnouncementTarget.InitWriter(c.ctx)
	if err != nil {
		c.log.Debug("could not initialize announcement accumulator",
			zap.Error(err),
		)

		return
	}

	// iterate over all collected metrics and write them to the target
	err = metricsIterator.Iterate(
		func(a container.SizeEstimation) bool {
			// local metrics don't know about epochs;
			// do not try to announce empty containers, it
			// takes additional network time but absolutely
			// useless
			return a.Value() != 0
		},
		func(a container.SizeEstimation) error {
			cnrString := a.Container().EncodeToString()

			c.log.Debug("sending local metrics", zap.String("cid", cnrString))

			a.SetEpoch(c.epoch) // set epoch explicitly
			err := targetWriter.Put(a)
			if err != nil {
				c.log.Warn("skip local metric", zap.String("cid", cnrString), zap.Error(err))
			}

			// estimations are too important to break the whole
			// process for the whole epoch because of any particular
			// error
			return nil
		},
	)
	if err != nil {
		c.log.Debug("iterator over locally collected metrics aborted",
			zap.Error(err),
		)

		return
	}

	// finish writing
	err = targetWriter.Close()
	if err != nil {
		c.log.Debug("could not finish writing local announcements",
			zap.Error(err),
		)

		return
	}

	c.log.Debug("local load announcement successfully finished")
}

func (c *Controller) acquireAnnouncement(prm StartPrm) *announceContext {
	var ctx context.Context

	c.announceMtx.Lock()

	{
		if cancel := c.mAnnounceCtx[prm.Epoch]; cancel == nil {
			ctx, cancel = context.WithCancel(context.Background())
			c.mAnnounceCtx[prm.Epoch] = cancel
		}
	}

	c.announceMtx.Unlock()

	log := c.opts.log.With(
		zap.Uint64("epoch", prm.Epoch),
		zap.String("stage", "p2p"),
	)

	if ctx == nil {
		log.Debug("local announcement is already started")
		return nil
	}

	return &announceContext{
		commonContext: commonContext{
			epoch: prm.Epoch,
			ctrl:  c,
			log:   log,
			ctx:   ctx,
		},
	}
}

func (c *commonContext) freeAnnouncement() {
	var stopped bool

	c.ctrl.announceMtx.Lock()

	{
		var cancel context.CancelFunc

		cancel, stopped = c.ctrl.mAnnounceCtx[c.epoch]

		if stopped {
			cancel()
			delete(c.ctrl.mAnnounceCtx, c.epoch)
		}
	}

	c.ctrl.announceMtx.Unlock()

	if stopped {
		c.log.Debug("announcement successfully interrupted")
	} else {
		c.log.Debug("announcement is not started or already interrupted")
	}
}

// StopPrm groups the required parameters of the Controller.Stop method.
type StopPrm struct {
	// Epoch number the analysis of the values of which must be interrupted.
	Epoch uint64
}

type stopContext struct {
	commonContext
}

// Stop interrupts the processing of container.SizeEstimation values.
//
// Single Stop operation releases an announcement context and overtakes
// all data from AnnouncementAccumulator to ResultReceiver (Controller's
// parameters). Only values for the specified Epoch parameter are processed.
//
// Each call acquires a report context for an Epoch parameter.
// At the very end of the operation, the context is released.
func (c *Controller) Stop(prm StopPrm) {
	execCtx := c.acquireReport(prm)
	if execCtx == nil {
		return
	}

	// finally stop and free reporting
	defer execCtx.freeReport()

	// interrupt announcement
	execCtx.freeAnnouncement()

	// report the estimations
	execCtx.report()
}

func (c *Controller) acquireReport(prm StopPrm) *stopContext {
	var ctx context.Context

	c.reportMtx.Lock()

	{
		if cancel := c.mReportCtx[prm.Epoch]; cancel == nil {
			ctx, cancel = context.WithCancel(context.Background())
			c.mReportCtx[prm.Epoch] = cancel
		}
	}

	c.reportMtx.Unlock()

	log := c.opts.log.With(
		zap.Uint64("epoch", prm.Epoch),
		zap.String("stage", "report"),
	)

	if ctx == nil {
		log.Debug("report is already started")
		return nil
	}

	return &stopContext{
		commonContext: commonContext{
			epoch: prm.Epoch,
			ctrl:  c,
			log:   log,
		},
	}
}

func (c *commonContext) freeReport() {
	var stopped bool

	c.ctrl.reportMtx.Lock()

	{
		var cancel context.CancelFunc

		cancel, stopped = c.ctrl.mReportCtx[c.epoch]

		if stopped {
			cancel()
			delete(c.ctrl.mReportCtx, c.epoch)
		}
	}

	c.ctrl.reportMtx.Unlock()

	if stopped {
		c.log.Debug("announcement successfully interrupted")
	} else {
		c.log.Debug("announcement is not started or already interrupted")
	}
}

func (c *stopContext) report() {
	var (
		localIterator Iterator
		err           error
	)

	// initialize iterator over locally accumulated announcements
	localIterator, err = c.ctrl.prm.AnnouncementAccumulator.InitIterator(c.ctx)
	if err != nil {
		c.log.Debug("could not initialize iterator over locally accumulated announcements",
			zap.Error(err),
		)

		return
	}

	// initialize final destination of load estimations
	resultWriter, err := c.ctrl.prm.ResultReceiver.InitWriter(c.ctx)
	if err != nil {
		c.log.Debug("could not initialize result target",
			zap.Error(err),
		)

		return
	}

	// iterate over all accumulated announcements and write them to the target
	err = localIterator.Iterate(
		usedSpaceFilterEpochEQ(c.epoch),
		resultWriter.Put,
	)
	if err != nil {
		c.log.Debug("iterator over local announcements aborted",
			zap.Error(err),
		)

		return
	}

	// finish writing
	err = resultWriter.Close()
	if err != nil {
		c.log.Debug("could not finish writing load estimations",
			zap.Error(err),
		)
	}
}
