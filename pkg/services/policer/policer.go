package policer

import (
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// Policer represents the utility that verifies
// compliance with the object storage policy.
type Policer struct {
	*cfg

	prevTask prevTask
}

// Option is an option for Policer constructor.
type Option func(*cfg)

// RedundantCopyCallback is a callback to pass
// the redundant local copy of the object.
type RedundantCopyCallback func(*object.Address)

type cfg struct {
	headTimeout time.Duration

	workScope workScope

	log *logger.Logger

	trigger <-chan *Task

	jobQueue jobQueue

	cnrSrc container.Source

	placementBuilder placement.Builder

	remoteHeader *headsvc.RemoteHeader

	netmapKeys netmap.AnnouncedKeys

	replicator *replicator.Replicator

	cbRedundantCopy RedundantCopyCallback
}

func defaultCfg() *cfg {
	return &cfg{
		log: zap.L(),
	}
}

// New creates, initializes and returns Policer instance.
func New(opts ...Option) *Policer {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	c.log = c.log.With(zap.String("component", "Object Policer"))

	return &Policer{
		cfg: c,
		prevTask: prevTask{
			cancel: func() {},
			wait:   new(sync.WaitGroup),
		},
	}
}

// WithHeadTimeout returns option to set Head timeout of Policer.
func WithHeadTimeout(v time.Duration) Option {
	return func(c *cfg) {
		c.headTimeout = v
	}
}

// WithWorkScope returns option to set job work scope value of Policer.
func WithWorkScope(v int) Option {
	return func(c *cfg) {
		c.workScope.val = v
	}
}

// WithExpansionRate returns option to set expansion rate of Policer's works scope (in %).
func WithExpansionRate(v int) Option {
	return func(c *cfg) {
		c.workScope.expRate = v
	}
}

// WithTrigger returns option to set triggering channel of Policer.
func WithTrigger(v <-chan *Task) Option {
	return func(c *cfg) {
		c.trigger = v
	}
}

// WithLogger returns option to set Logger of Policer.
func WithLogger(v *logger.Logger) Option {
	return func(c *cfg) {
		c.log = v
	}
}

// WithLocalStorage returns option to set local object storage of Policer.
func WithLocalStorage(v *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.jobQueue.localStorage = v
	}
}

// WithContainerSource returns option to set container source of Policer.
func WithContainerSource(v container.Source) Option {
	return func(c *cfg) {
		c.cnrSrc = v
	}
}

// WithPlacementBuilder returns option to set object placement builder of Policer.
func WithPlacementBuilder(v placement.Builder) Option {
	return func(c *cfg) {
		c.placementBuilder = v
	}
}

// WithRemoteHeader returns option to set object header receiver of Policer.
func WithRemoteHeader(v *headsvc.RemoteHeader) Option {
	return func(c *cfg) {
		c.remoteHeader = v
	}
}

// WithNetmapKeys returns option to set tool to work with announced public keys.
func WithNetmapKeys(v netmap.AnnouncedKeys) Option {
	return func(c *cfg) {
		c.netmapKeys = v
	}
}

// WithReplicator returns option to set object replicator of Policer.
func WithReplicator(v *replicator.Replicator) Option {
	return func(c *cfg) {
		c.replicator = v
	}
}

// WithRedundantCopyCallback returns option to set
// callback to pass redundant local object copies
// detected by Policer.
func WithRedundantCopyCallback(cb RedundantCopyCallback) Option {
	return func(c *cfg) {
		c.cbRedundantCopy = cb
	}
}
