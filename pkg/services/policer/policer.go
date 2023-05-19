package policer

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// NodeLoader provides application load statistics.
type nodeLoader interface {
	// ObjectServiceLoad returns object service load value in [0:1] range.
	ObjectServiceLoad() float64
}

type objectsInWork struct {
	m    sync.RWMutex
	objs map[oid.Address]struct{}
}

func (oiw *objectsInWork) inWork(addr oid.Address) bool {
	oiw.m.RLock()
	_, ok := oiw.objs[addr]
	oiw.m.RUnlock()

	return ok
}

func (oiw *objectsInWork) remove(addr oid.Address) {
	oiw.m.Lock()
	delete(oiw.objs, addr)
	oiw.m.Unlock()
}

func (oiw *objectsInWork) add(addr oid.Address) {
	oiw.m.Lock()
	oiw.objs[addr] = struct{}{}
	oiw.m.Unlock()
}

// Policer represents the utility that verifies
// compliance with the object storage policy.
type Policer struct {
	*cfg

	cache *lru.Cache[oid.Address, time.Time]

	objsInWork *objectsInWork
}

// Option is an option for Policer constructor.
type Option func(*cfg)

// RedundantCopyCallback is a callback to pass
// the redundant local copy of the object.
type RedundantCopyCallback func(oid.Address)

// Network provides information about the NeoFS network to Policer for work.
type Network interface {
	// IsLocalNodeInNetmap checks whether the local node belongs to the current
	// network map. If it is impossible to check this fact, IsLocalNodeInNetmap
	// returns false.
	IsLocalNodeInNetmap() bool
}

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

	network Network
}

func defaultCfg() *cfg {
	return &cfg{
		log:           &logger.Logger{Logger: zap.L()},
		batchSize:     10,
		cacheSize:     1024, // 1024 * address size = 1024 * 64 = 64 MiB
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

	c.log = &logger.Logger{Logger: c.log.With(zap.String("component", "Object Policer"))}

	cache, err := lru.New[oid.Address, time.Time](int(c.cacheSize))
	if err != nil {
		panic(err)
	}

	return &Policer{
		cfg:   c,
		cache: cache,
		objsInWork: &objectsInWork{
			objs: make(map[oid.Address]struct{}, c.maxCapacity),
		},
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
func WithMaxCapacity(capacity int) Option {
	return func(c *cfg) {
		c.maxCapacity = capacity
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

// WithNetwork provides Network component.
func WithNetwork(n Network) Option {
	return func(c *cfg) {
		c.network = n
	}
}
