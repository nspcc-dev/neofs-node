package audittask

import (
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit/auditor"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// Manager represents an entity performing data audit tasks.
type Manager struct {
	*cfg

	ch chan *audit.Task
}

// Option is a Manager's constructor option.
type Option func(*cfg)

type cfg struct {
	queueCap uint32

	log *logger.Logger

	ctxPrm auditor.ContextPrm

	workerPool *ants.Pool

	pdpPoolSize, porPoolSize int
}

func defaultCfg() *cfg {
	return &cfg{
		log: &logger.Logger{Logger: zap.L()},
	}
}

// New creates, initializes and returns new Manager instance.
func New(opts ...Option) *Manager {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Manager{
		cfg: c,
	}
}

// WithLogger returns option to specify Manager's logger.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = &logger.Logger{Logger: l.With(zap.String("component", "Audit task manager"))}
		c.ctxPrm.SetLogger(l)
	}
}

// WithWorkerPool returns option to set worker pool
// for task execution.
func WithWorkerPool(p *ants.Pool) Option {
	return func(c *cfg) {
		c.workerPool = p
	}
}

// WithQueueCapacity returns option to set task queue capacity.
func WithQueueCapacity(capacity uint32) Option {
	return func(c *cfg) {
		c.queueCap = capacity
	}
}

// WithContainerCommunicator returns option to set component of communication
// with container nodes.
func WithContainerCommunicator(cnrCom auditor.ContainerCommunicator) Option {
	return func(c *cfg) {
		c.ctxPrm.SetContainerCommunicator(cnrCom)
	}
}

// WithMaxPDPSleepInterval returns option to set maximum sleep interval
// between range hash requests as part of PDP check.
func WithMaxPDPSleepInterval(dur time.Duration) Option {
	return func(c *cfg) {
		c.ctxPrm.SetMaxPDPSleep(dur)
	}
}

// WithPDPWorkerPoolSize returns option to set worker pool size for PDP pairs processing.
// Non-positive value means infinite pool.
func WithPDPWorkerPoolSize(sz int) Option {
	return func(c *cfg) {
		c.pdpPoolSize = sz
	}
}

// WithPoRWorkerPoolSize returns option to set worker pool size for PoR SG processing.
// Non-positive value means infinite pool.
func WithPoRWorkerPoolSize(sz int) Option {
	return func(c *cfg) {
		c.porPoolSize = sz
	}
}
