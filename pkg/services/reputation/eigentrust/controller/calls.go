package eigentrustctrl

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
	"go.uber.org/zap"
)

// ContinuePrm groups the required parameters of Continue operation.
type ContinuePrm struct {
	epoch uint64
}

type iterContext struct {
	context.Context

	eigentrust.EpochIteration

	last bool
}

func (x iterContext) Last() bool {
	return x.last
}

type iterContextCancel struct {
	iterContext

	cancel context.CancelFunc
}

// Continue moves the global reputation calculator to the next iteration.
func (c *Controller) Continue(prm ContinuePrm) {
	c.mtx.Lock()

	{
		iterCtx, ok := c.mCtx[prm.epoch]
		if !ok {
			iterCtx := new(iterContextCancel)
			c.mCtx[prm.epoch] = iterCtx

			iterCtx.Context, iterCtx.cancel = context.WithCancel(context.Background())
		} else {
			iterCtx.cancel()
		}

		iterCtx.last = iterCtx.I() == c.prm.IterationNumber

		err := c.prm.WorkerPool.Submit(func() {
			c.prm.DaughtersTrustCalculator.Calculate(iterCtx.iterContext)
		})
		if err != nil {
			c.opts.log.Debug("iteration submit failure",
				zap.String("error", err.Error()),
			)
		}

		if iterCtx.last {
			delete(c.mCtx, prm.epoch)
			// In this case and worker pool failure we can mark epoch
			// number as already processed, but in any case it grows up
			// during normal operation of the system. Also, such information
			// will only live while the application is alive.
		}
	}

	c.mtx.Unlock()
}
