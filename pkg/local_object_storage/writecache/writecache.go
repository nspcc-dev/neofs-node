package writecache

import (
	"fmt"
	"io"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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
	// GetStream returns an object and a stream to read its payload.
	GetStream(oid.Address) (*object.Object, io.ReadCloser, error)
	GetRangeStream(addr oid.Address, off uint64, ln uint64) (io.ReadCloser, error)
	Head(oid.Address) (*object.Object, error)
	HeadToBuffer(oid.Address, func() []byte) (int, error)
	// Delete removes object referenced by the given oid.Address from the
	// Cache. Returns any error encountered that prevented the object to be
	// removed.
	//
	// Returns apistatus.ObjectNotFound if object is missing in the Cache.
	// Returns ErrReadOnly if the Cache is currently in the read-only mode.
	Delete(oid.Address) error
	Iterate(func(oid.Address, []byte) error, bool) error
	Put(oid.Address, *object.Object, []byte) error
	SetMode(mode.Mode) error
	SetLogger(*zap.Logger)
	SetShardIDMetrics(string)
	DumpInfo() Info
	Flush(bool) error

	Init() error
	Open(readOnly bool) error
	Close() error
	ObjectStatus(address oid.Address) (ObjectStatus, error)
}

type cache struct {
	options

	mode    mode.Mode
	modeMtx sync.RWMutex

	// flushErrCh is a channel for error handling while flushing.
	flushErrCh chan struct{}

	// flushCh is a channel with objects to flush.
	flushCh chan []oid.Address
	// flushObjs is a map with objects that are currently being processed by flusher.
	flushObjs sync.Map
	// closeCh is close channel.
	closeCh chan struct{}
	// wg is a wait group for flush workers.
	wg sync.WaitGroup
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
	defaultMaxCacheSize = 1 << 30 // 1 GiB
)

// New creates new writecache instance.
func New(opts ...Option) Cache {
	c := &cache{
		flushCh:    make(chan []oid.Address),
		flushErrCh: make(chan struct{}, 1),
		mode:       mode.ReadWrite,

		options: options{
			log:          zap.NewNop(),
			metrics:      new(metricsWithID),
			maxCacheSize: defaultMaxCacheSize,
			objCounters: counters{
				objMap: make(map[oid.Address]uint64),
			},
			workersCount:           defaultWorkerCount,
			maxFlushBatchSize:      defaultMaxBatchSize,
			maxFlushBatchCount:     defaultMaxBatchCount,
			maxFlushBatchThreshold: defaultMaxBatchTreshold,
		},
	}

	for i := range opts {
		opts[i](&c.options)
	}

	return c
}

// SetLogger sets logger. It is used after the shard ID was generated to use it in logs.
func (c *cache) SetLogger(l *zap.Logger) {
	c.log = l.With(zap.String("substorage", wcStorageType))
}

// SetShardIDMetrics sets shard id for metrics. It is used after the shard ID was generated.
func (c *cache) SetShardIDMetrics(id string) {
	c.metrics.id = id
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

	c.modeMtx.Lock()
	if readOnly {
		c.mode = mode.ReadOnly
	} else {
		c.mode = mode.ReadWrite
	}
	c.modeMtx.Unlock()

	return c.initCounters()
}

// Init runs necessary services. No-op in read-only mode.
func (c *cache) Init() error {
	err := c.fsTree.Init()
	if err != nil {
		return fmt.Errorf("init FSTree: %w", err)
	}

	c.modeMtx.Lock()
	defer c.modeMtx.Unlock()

	if !c.readOnly() {
		c.runFlushLoop()
	}
	return nil
}

// Close stops services. Executes ObjectCounters.FlushAndClose op.
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

	return nil
}
