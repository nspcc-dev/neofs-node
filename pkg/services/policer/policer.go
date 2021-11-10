package policer

import (
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// NodeLoader provides application load statistics.
type nodeLoader interface {
	// ObjectServiceLoad returns object service load value in [0:1] range.
	ObjectServiceLoad() float64
}

// Policer represents the utility that verifies
// compliance with the object storage policy.
type Policer struct {
	*cfg

	cache *lru.Cache
}

// Option is an option for Policer constructor.
type Option func(*cfg)

// RedundantCopyCallback is a callback to pass
// the redundant local copy of the object.
type RedundantCopyCallback func(*object.Address)

type cfg struct {
	headTimeout time.Duration

	log *logger.Logger

	jobQueue jobQueue

	cnrSrc container.Source

	placementBuilder placement.Builder

	remoteHeader *headsvc.RemoteHeader

	netmapKeys netmap.AnnouncedKeys

	replicator *replicator.Replicator

	cbRedundantCopy RedundantCopyCallback

	taskPool *ants.Pool

	loader nodeLoader

	maxCapacity int

	batchSize, cacheSize uint32

	rebalanceFreq, evictDuration time.Duration
}

func defaultCfg() *cfg {
	return &cfg{
		log:           zap.L(),
		batchSize:     10,
		cacheSize:     200_000, // should not allocate more than 200 MiB
		rebalanceFreq: 1 * time.Second,
		evictDuration: 30 * time.Second,
	}
}

// New creates, initializes and returns Policer instance.
func New(opts ...Option) *Policer {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	c.log = c.log.With(zap.String("component", "Object Policer"))

	cache, err := lru.New(int(c.cacheSize))
	if err != nil {
		panic(err)
	}

	return &Policer{
		cfg:   c,
		cache: cache,
	}
}

// WithHeadTimeout returns option to set Head timeout of Policer.
func WithHeadTimeout(v time.Duration) Option {
	return func(c *cfg) {
		c.headTimeout = v
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

// WithMaxCapacity returns option to set max capacity
// that can be set to the pool.
func WithMaxCapacity(cap int) Option {
	return func(c *cfg) {
		c.maxCapacity = cap
	}
}

// WithPool returns option to set pool for
// policy and replication operations.
func WithPool(p *ants.Pool) Option {
	return func(c *cfg) {
		c.taskPool = p
	}
}

// WithNodeLoader returns option to set NeoFS node load source.
func WithNodeLoader(l nodeLoader) Option {
	return func(c *cfg) {
		c.loader = l
	}
}
