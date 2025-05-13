package settlement

import (
	"fmt"

	nodeutil "github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type (
	// AlphabetState is a callback interface for inner ring global state.
	AlphabetState interface {
		IsAlphabet() bool
	}

	// Processor is an event handler for payments in the system.
	Processor struct {
		log *zap.Logger

		state AlphabetState

		pool nodeutil.WorkerPool

		auditProc AuditProcessor

		basicIncome BasicIncomeInitializer
	}

	// Prm groups the required parameters of Processor's constructor.
	Prm struct {
		AuditProcessor AuditProcessor
		BasicIncome    BasicIncomeInitializer
		State          AlphabetState
	}
)

func panicOnPrmValue(n string, v any) {
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
		panic(fmt.Errorf("could not create worker pool: %w", err))
	}

	o.log.Debug("worker pool for settlement processor successfully initialized",
		zap.Int("capacity", o.poolSize),
	)

	return &Processor{
		log:         o.log,
		state:       prm.State,
		pool:        pool,
		auditProc:   prm.AuditProcessor,
		basicIncome: prm.BasicIncome,
	}
}
