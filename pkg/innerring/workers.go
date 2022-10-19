package innerring

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// workers implements worker pool for the Inner Ring app. The pool has fixed
// number of workers.
type workers struct {
	log *logger.Logger

	ants *ants.Pool
}

// init initializes the workers instance. Number of workers MUST NOT be zero.
// Logger MUST NOT be nil.
func (x *workers) init(workersNum uint, l *logger.Logger) {
	switch {
	case workersNum == 0:
		panic("zero number of workers")
	case l == nil:
		panic("missing logger")
	}

	var err error

	x.ants, err = ants.NewPool(int(workersNum), ants.WithNonblocking(true))
	if err != nil {
		panic(fmt.Sprintf("init ants worker pool: %v", err))
	}

	x.log = l
}

// Submit schedules the given job to be asynchronously executed. If there are
// no free workers, Submit logs this fact.
func (x *workers) Submit(job func()) {
	err := x.ants.Submit(job)
	if err != nil {
		if errors.Is(err, ants.ErrPoolOverload) {
			x.log.Warn("worker pool is full")
			return
		}

		x.log.Error("failed to submit job to the worker pool",
			zap.Error(err),
		)
	}
}
