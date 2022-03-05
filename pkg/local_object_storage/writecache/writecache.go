package writecache

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.uber.org/zap"
)

// Info groups the information about write-cache.
type Info struct {
	// Full path to the write-cache.
	Path string
}

// Cache represents write-cache for objects.
type Cache interface {
	Get(*addressSDK.Address) (*object.Object, error)
	Head(*addressSDK.Address) (*object.Object, error)
	Delete(*addressSDK.Address) error
	Iterate(*IterationPrm) error
	Put(*object.Object) error
	SetMode(Mode)
	DumpInfo() Info

	Init() error
	Open() error
	Close() error
}

type cache struct {
	options

	// mtx protects mem field, statistics, counters and compressFlags.
	mtx sync.RWMutex
	mem []objectInfo

	mode    Mode
	modeMtx sync.RWMutex

	// compressFlags maps address of a big object to boolean value indicating
	// whether object should be compressed.
	compressFlags map[string]struct{}

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
	evictCh chan []byte
	// store contains underlying database.
	store
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
	maxCacheSizeBytes    = 1 << 30            // 1 GiB
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
		evictCh:  make(chan []byte),
		mode:     ModeReadWrite,

		compressFlags: make(map[string]struct{}),
		options: options{
			log:             zap.NewNop(),
			maxMemSize:      maxInMemorySizeBytes,
			maxObjectSize:   maxObjectSize,
			smallObjectSize: smallObjectSize,
			workersCount:    flushWorkersCount,
			maxCacheSize:    maxCacheSizeBytes,
		},
	}

	for i := range opts {
		opts[i](&c.options)
	}

	return c
}

func (c *cache) DumpInfo() Info {
	return Info{
		Path: c.path,
	}
}

// Open opens and initializes database. Reads object counters from the ObjectCounters instance.
func (c *cache) Open() error {
	err := c.openStore()
	if err != nil {
		return err
	}

	if c.objCounters == nil {
		c.objCounters = &counters{
			db: c.db,
			fs: c.fsTree,
		}
	}

	return c.objCounters.Read()
}

// Init runs necessary services.
func (c *cache) Init() error {
	go c.persistLoop()
	go c.flushLoop()
	return nil
}

// Close closes db connection and stops services. Executes ObjectCounters.FlushAndClose op.
func (c *cache) Close() error {
	// Finish all in-progress operations.
	c.SetMode(ModeReadOnly)

	close(c.closeCh)
	c.objCounters.FlushAndClose()
	return c.db.Close()
}
