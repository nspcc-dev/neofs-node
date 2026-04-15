package replicator

import (
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"go.uber.org/zap"
)

const (
	// defaultQueueSize is the default capacity of an asynchronous replication queue.
	defaultQueueSize = 4096
	// defaultWorkers is the default number of background replication workers.
	defaultWorkers = 8
)

// LocalNodeKey provides information about the NeoFS network to Replicator for work.
type LocalNodeKey interface {
	// IsLocalNodePublicKey checks whether given binary-encoded public key is
	// assigned in the network map to a local storage node running [Replicator].
	IsLocalNodePublicKey([]byte) bool
}

// Replicator represents the utility that replicates
// local objects to remote nodes.
type Replicator struct {
	*cfg

	taskQueue chan Task
}

// Option is an option for Policer constructor.
type Option func(*cfg)

type cfg struct {
	putTimeout time.Duration

	log *zap.Logger

	remoteSender *putsvc.RemoteSender

	localStorage *engine.StorageEngine

	localNodeKey LocalNodeKey
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
		cfg:       c,
		taskQueue: make(chan Task, defaultQueueSize),
	}
}

// WithPutTimeout returns option to set Put timeout of Replicator.
func WithPutTimeout(v time.Duration) Option {
	return func(c *cfg) {
		c.putTimeout = v
	}
}

// WithLogger returns option to set Logger of Replicator.
func WithLogger(v *zap.Logger) Option {
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

// WithLocalNodeKey provides LocalNodeKey component.
func WithLocalNodeKey(n LocalNodeKey) Option {
	return func(c *cfg) {
		c.localNodeKey = n
	}
}
