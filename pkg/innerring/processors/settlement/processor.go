package settlement

import (
	"fmt"

	nodeutil "github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Processor is an event handler for payments in the system.
type Processor struct {
	log *logger.Logger

	pool nodeutil.WorkerPool

	auditProc AuditProcessor
}

// Prm groups the required parameters of Processor's constructor.
type Prm struct {
	AuditProcessor AuditProcessor
}

func panicOnPrmValue(n string, v interface{}) {
	panic(fmt.Sprintf("invalid parameter %s (%T):%v", n, v, v))
}

// New creates and returns a new Processor instance.
func New(prm Prm, opts ...Option) *Processor {
	switch {
	case prm.AuditProcessor == nil:
		panicOnPrmValue("AuditProcessor", prm.AuditProcessor)
	}

	o := defaultOptions()

	for i := range opts {
		opts[i](o)
	}

	pool, err := ants.NewPool(o.poolSize, ants.WithNonblocking(true))
	if err != nil {
		panic(errors.Wrap(err, "could not create worker pool"))
	}

	o.log.Debug("worker pool for settlement processor successfully initialized",
		zap.Int("capacity", o.poolSize),
	)

	return &Processor{
		log:       o.log,
		pool:      pool,
		auditProc: prm.AuditProcessor,
	}
}
