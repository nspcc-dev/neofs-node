package replicator

import (
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// Replicator represents the utility that replicates
// local objects to remote nodes.
type Replicator struct {
	*cfg

	ch chan *Task
}

// Option is an option for Policer constructor.
type Option func(*cfg)

type cfg struct {
	taskCap uint32

	putTimeout time.Duration

	log *logger.Logger

	remoteSender *putsvc.RemoteSender

	localStorage *engine.StorageEngine
}

func defaultCfg() *cfg {
	return &cfg{}
}

// New creates, initializes and returns Replicator instance.
func New(opts ...Option) *Replicator {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	c.log = c.log.With(zap.String("component", "Object Replicator"))

	return &Replicator{
		cfg: c,
	}
}

// WithPutTimeout returns option to set Put timeout of Replicator.
func WithPutTimeout(v time.Duration) Option {
	return func(c *cfg) {
		c.putTimeout = v
	}
}

// WithLogger returns option to set Logger of Replicator.
func WithLogger(v *logger.Logger) Option {
	return func(c *cfg) {
		c.log = v
	}
}

// WithRemoteSender returns option to set remote object sender of Replicator.
func WithRemoteSender(v *putsvc.RemoteSender) Option {
	return func(c *cfg) {
		c.remoteSender = v
	}
}

// WithLocalStorage returns option to set local object storage of Replicator.
func WithLocalStorage(v *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.localStorage = v
	}
}
