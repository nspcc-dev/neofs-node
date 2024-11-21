package writecache

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// Info groups the information about write-cache.
type Info struct {
	// Full path to the write-cache.
	Path string
}

// Cache represents write-cache for objects.
type Cache interface {
	Get(address oid.Address) (*object.Object, error)
	// GetBytes reads object from the Cache by address into memory buffer in a
	// canonical NeoFS binary format. Returns [apistatus.ObjectNotFound] if object
	// is missing.
	GetBytes(oid.Address) ([]byte, error)
	Head(oid.Address) (*object.Object, error)
	// Delete removes object referenced by the given oid.Address from the
	// Cache. Returns any error encountered that prevented the object to be
	// removed.
	//
	// Returns apistatus.ObjectNotFound if object is missing in the Cache.
	// Returns ErrReadOnly if the Cache is currently in the read-only mode.
	Delete(oid.Address) error
	Iterate(func(oid.Address, []byte) error, bool) error
	Put(common.PutPrm) (common.PutRes, error)
	SetMode(mode.Mode) error
	SetLogger(*zap.Logger)
	DumpInfo() Info
	Flush(bool) error

	Init() error
	Open(readOnly bool) error
	Close() error
	ObjectStatus(address oid.Address) (ObjectStatus, error)
}

type cache struct {
	options

	// mtx protects statistics, counters and compressFlags.
	mtx sync.RWMutex

	mode    mode.Mode
	modeMtx sync.RWMutex

	// compressFlags maps address of a big object to boolean value indicating
	// whether object should be compressed.
	compressFlags map[string]struct{}

	// flushCh is a channel with objects to flush.
	flushCh chan *object.Object
	// closeCh is close channel.
	closeCh chan struct{}
	// wg is a wait group for flush workers.
	wg sync.WaitGroup
	// store contains underlying database.
	store
	// fsTree contains big files stored directly on file-system.
	fsTree *fstree.FSTree
}

// wcStorageType is used for write-cache operations logging.
const wcStorageType = "write-cache"

type objectInfo struct {
	addr string
	data []byte
	obj  *object.Object
}

const (
	defaultMaxObjectSize   = 64 * 1024 * 1024 // 64 MiB
	defaultSmallObjectSize = 32 * 1024        // 32 KiB
	defaultMaxCacheSize    = 1 << 30          // 1 GiB
)

var (
	defaultBucket = []byte{0}
)

// New creates new writecache instance.
func New(opts ...Option) Cache {
	c := &cache{
		flushCh: make(chan *object.Object),
		mode:    mode.ReadWrite,

		compressFlags: make(map[string]struct{}),
		options: options{
			log:             zap.NewNop(),
			maxObjectSize:   defaultMaxObjectSize,
			smallObjectSize: defaultSmallObjectSize,
			workersCount:    defaultFlushWorkersCount,
			maxCacheSize:    defaultMaxCacheSize,
			maxBatchSize:    bbolt.DefaultMaxBatchSize,
			maxBatchDelay:   bbolt.DefaultMaxBatchDelay,
		},
	}

	for i := range opts {
		opts[i](&c.options)
	}

	// Make the LRU cache contain which take approximately 3/4 of the maximum space.
	// Assume small and big objects are stored in 50-50 proportion.
	c.maxFlushedMarksCount = int(c.maxCacheSize/c.maxObjectSize+c.maxCacheSize/c.smallObjectSize) / 2 * 3 / 4
	// Trigger the removal when the cache is 7/8 full, so that new items can still arrive.
	c.maxRemoveBatchSize = c.maxFlushedMarksCount / 8

	return c
}

// SetLogger sets logger. It is used after the shard ID was generated to use it in logs.
func (c *cache) SetLogger(l *zap.Logger) {
	c.log = l
}

func (c *cache) DumpInfo() Info {
	return Info{
		Path: c.path,
	}
}

// Open opens and initializes database. Reads object counters from the ObjectCounters instance.
func (c *cache) Open(readOnly bool) error {
	err := c.openStore(readOnly)
	if err != nil {
		return err
	}

	// Opening after Close is done during maintenance mode,
	// thus we need to create a channel here.
	c.closeCh = make(chan struct{})

	return c.initCounters()
}

// Init runs necessary services. No-op in read-only mode.
func (c *cache) Init() error {
	if !c.db.IsReadOnly() {
		c.initFlushMarks()
		c.runFlushLoop()
	}
	return nil
}

// Close closes db connection and stops services. Executes ObjectCounters.FlushAndClose op.
func (c *cache) Close() error {
	// Finish all in-progress operations.
	if err := c.SetMode(mode.ReadOnly); err != nil {
		return err
	}

	if c.closeCh != nil {
		close(c.closeCh)
	}
	c.wg.Wait()
	if c.closeCh != nil {
		c.closeCh = nil
	}

	var err error
	if c.db != nil {
		err = c.db.Close()
		if err != nil {
			c.db = nil
		}
	}
	return nil
}
