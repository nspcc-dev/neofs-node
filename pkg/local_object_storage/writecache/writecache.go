package writecache

import (
	"sync"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Cache represents write-cache for objects.
type Cache interface {
	Get(*objectSDK.Address) (*object.Object, error)
	Delete(*objectSDK.Address) error
	Put(*object.Object) error

	Init() error
	Open() error
	Close() error
}

type cache struct {
	options

	// mtx protects mem field, statistics and counters.
	mtx sync.RWMutex
	mem []objectInfo

	// curMemSize is the current size of all objects cached in memory.
	curMemSize uint64

	// flushCh is a channel with objects to flush.
	flushCh chan *object.Object
	// directCh is a channel with objects to put directly to the main storage.
	// it is prioritized over flushCh.
	directCh chan *object.Object
	// metaCh is a channel with objects for which only metadata needs to be written.
	metaCh chan *object.Object
	// closeCh is close channel.
	closeCh chan struct{}
	waitCh  chan struct{}
	evictCh chan []byte
	// store contains underlying database.
	store
	// dbSize stores approximate database size. It is updated every flush/persist cycle.
	dbSize atomic.Uint64
	// fsTree contains big files stored directly on file-system.
	fsTree *fstree.FSTree
}

type objectInfo struct {
	addr string
	data []byte
	obj  *object.Object
}

const (
	maxInMemorySizeBytes = 1024 * 1024 * 1024 // 1 GiB
	maxObjectSize        = 64 * 1024 * 1024   // 64 MiB
	smallObjectSize      = 32 * 1024          // 32 KiB
)

var (
	defaultBucket = []byte{0}
)

// New creates new writecache instance.
func New(opts ...Option) Cache {
	c := &cache{
		flushCh:  make(chan *object.Object),
		directCh: make(chan *object.Object),
		metaCh:   make(chan *object.Object),
		closeCh:  make(chan struct{}),
		waitCh:   make(chan struct{}),
		evictCh:  make(chan []byte),

		options: options{
			log:             zap.NewNop(),
			maxMemSize:      maxInMemorySizeBytes,
			maxObjectSize:   maxObjectSize,
			smallObjectSize: smallObjectSize,
			workersCount:    flushWorkersCount,
		},
	}

	for i := range opts {
		opts[i](&c.options)
	}

	return c
}

// Open opens and initializes database.
func (c *cache) Open() error {
	return c.openStore()
}

// Init runs necessary services.
func (c *cache) Init() error {
	go c.persistLoop()
	go c.flushLoop()
	return nil
}

// Close closes db connection and stops services.
func (c *cache) Close() error {
	close(c.closeCh)
	<-c.waitCh
	return c.db.Close()
}
