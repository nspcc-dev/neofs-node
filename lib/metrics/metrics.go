package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/lib/meta"
	"go.uber.org/zap"
)

type (
	// Collector is an interface of the metrics collector.
	Collector interface {
		Start(ctx context.Context)
		UpdateSpaceUsage()

		SetCounter(ObjectCounter)
		SetIterator(iter meta.Iterator)
		UpdateContainer(cid refs.CID, size uint64, op SpaceOp)
	}

	collector struct {
		log      *zap.Logger
		interval time.Duration
		counter  *counterWrapper

		sizes *syncStore
		metas *metaWrapper

		updateSpaceSize   func()
		updateObjectCount func()
	}

	// Params groups the parameters of metrics collector's constructor.
	Params struct {
		Options      []string
		Logger       *zap.Logger
		Interval     time.Duration
		MetricsStore core.Bucket
	}

	// ObjectCounter is an interface of object number storage.
	ObjectCounter interface {
		ObjectsCount() (uint64, error)
	}

	// CounterSetter is an interface of ObjectCounter container.
	CounterSetter interface {
		SetCounter(ObjectCounter)
	}

	counterWrapper struct {
		sync.Mutex
		counter ObjectCounter
	}
)

const (
	errEmptyCounter      = internal.Error("empty object counter")
	errEmptyLogger       = internal.Error("empty logger")
	errEmptyMetaStore    = internal.Error("empty meta store")
	errEmptyMetricsStore = internal.Error("empty metrics store")

	defaultMetricsInterval = 5 * time.Second
)

// New constructs metrics collector and returns Collector interface.
func New(p Params) (Collector, error) {
	switch {
	case p.Logger == nil:
		return nil, errEmptyLogger
	case p.MetricsStore == nil:
		return nil, errEmptyMetricsStore
	}

	if p.Interval <= 0 {
		p.Interval = defaultMetricsInterval
	}

	metas := newMetaWrapper()
	sizes := newSyncStore(p.Logger, p.MetricsStore)

	sizes.Load()

	return &collector{
		log:      p.Logger,
		interval: p.Interval,
		counter:  new(counterWrapper),

		metas: metas,
		sizes: sizes,

		updateSpaceSize:   spaceUpdater(sizes),
		updateObjectCount: metricsUpdater(p.Options),
	}, nil
}

func (c *counterWrapper) SetCounter(counter ObjectCounter) {
	c.Lock()
	defer c.Unlock()

	c.counter = counter
}

func (c *counterWrapper) ObjectsCount() (uint64, error) {
	c.Lock()
	defer c.Unlock()

	if c.counter == nil {
		return 0, errEmptyCounter
	}

	return c.counter.ObjectsCount()
}

func (c *collector) SetCounter(counter ObjectCounter) {
	c.counter.SetCounter(counter)
}

func (c *collector) SetIterator(iter meta.Iterator) {
	c.metas.changeIter(iter)
}

func (c *collector) UpdateContainer(cid refs.CID, size uint64, op SpaceOp) {
	c.sizes.Update(cid, size, op)
	c.updateSpaceSize()
}

func (c *collector) UpdateSpaceUsage() {
	sizes := make(map[refs.CID]uint64)

	err := c.metas.Iterate(func(obj *object.Object) error {
		if !obj.IsTombstone() {
			cid := obj.SystemHeader.CID
			sizes[cid] += obj.SystemHeader.PayloadLength
		}

		return nil
	})

	if err != nil {
		c.log.Error("could not update space metrics", zap.Error(err))
	}

	c.sizes.Reset(sizes)
	c.updateSpaceSize()
}

func (c *collector) Start(ctx context.Context) {
	t := time.NewTicker(c.interval)

loop:
	for {
		select {
		case <-ctx.Done():
			c.log.Warn("stop collecting metrics", zap.Error(ctx.Err()))
			break loop
		case <-t.C:
			count, err := c.counter.ObjectsCount()
			if err != nil {
				c.log.Warn("get object count failure", zap.Error(err))
				continue loop
			}
			counter.Store(float64(count))
			c.updateObjectCount()
		}
	}

	t.Stop()
}
