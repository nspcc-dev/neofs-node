package eigentrustctrl

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
	"go.uber.org/zap"
)

// ContinuePrm groups the required parameters of Continue operation.
type ContinuePrm struct {
	Epoch uint64
}

type iterContext struct {
	context.Context

	eigentrust.EpochIteration

	iterationNumber uint32
	last            bool
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
		iterCtx, ok := c.mCtx[prm.Epoch]
		if !ok {
			iterCtx = new(iterContextCancel)
			c.mCtx[prm.Epoch] = iterCtx

			iterCtx.Context, iterCtx.cancel = context.WithCancel(context.Background())
			iterCtx.SetEpoch(prm.Epoch)

			iterations, err := c.prm.IterationsProvider.EigenTrustIterations()
			if err != nil {
				c.opts.log.Error("could not get EigenTrust iteration number",
					zap.Error(err),
				)
			} else {
				iterCtx.iterationNumber = uint32(iterations)
			}
		} else {
			iterCtx.cancel()
		}

		iterCtx.last = iterCtx.I() == iterCtx.iterationNumber-1

		err := c.prm.WorkerPool.Submit(func() {
			c.prm.DaughtersTrustCalculator.Calculate(iterCtx.iterContext)

			// iteration++
			iterCtx.Increment()
		})
		if err != nil {
			c.opts.log.Debug("iteration submit failure",
				zap.String("error", err.Error()),
			)
		}

		if iterCtx.last {
			// will only live while the application is alive.
			// during normal operation of the system. Also, such information
			// number as already processed, but in any case it grows up
			// In this case and worker pool failure we can mark epoch
			delete(c.mCtx, prm.Epoch)
		}
	}

	c.mtx.Unlock()
}
