package writecache

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

const (
	// defaultErrorDelay is default time delay for retrying a flush after receiving an error.
	defaultErrorDelay = 10 * time.Second
	// defaultWorkerCount is a default number of workers that flush objects.
	defaultWorkerCount = 20
	// defaultMaxBatchDelay is a default delay before pushing small objects.
	defaultMaxBatchDelay = 1 * time.Second
	// defaultMaxBatchSize is a default maximum size at which the batch is flushed.
	defaultMaxBatchSize = 8 * 1024 * 1024
	// defaultMaxBatchCount is a default maximum count of small object that is flushed in batch.
	defaultMaxBatchCount = 128
	// defaultMaxBatchTreshold is a default maximum size of small object that put in a batch.
	defaultMaxBatchTreshold = 128 * 1024
)

// batch is a set of object to write in a batch, writing is triggered
// by size/count overflow or timer.
type batch struct {
	c     *cache
	timer *time.Timer

	m     sync.Mutex
	addrs []oid.Address
	done  bool
	size  uint64
}

func (c *cache) newBatch() *batch {
	var b = &batch{c: c}

	b.timer = time.AfterFunc(defaultMaxBatchDelay, b.flush)
	return b
}

func (b *batch) flush() {
	b.m.Lock()
	defer b.m.Unlock()
	if b.done {
		return
	}

	// Can be triggered by timer or via other means,
	// but timer is irrelevant now anyway.
	_ = b.timer.Stop()
	b.done = true

	if len(b.addrs) == 0 {
		return
	}

	err := b.c.flushBatch(b.addrs)
	if err != nil {
		b.c.log.Error("can't flush batch of objects", zap.Error(err))
		select {
		case b.c.flushErrCh <- struct{}{}:
		default:
		}
	}
}

// addObject add an object to batch if batch is active, returns success
// (added or not) and batch completion status (true if this batch no longer
// accepts objects).
func (b *batch) addObject(a oid.Address, s uint64) (bool, bool) {
	b.m.Lock()
	defer b.m.Unlock()

	if b.done {
		return false, true
	}

	b.addrs = append(b.addrs, a)
	b.size += s
	if b.size >= b.c.maxFlushBatchSize || len(b.addrs) > b.c.maxFlushBatchCount {
		go b.flush()
		return true, true
	}
	return true, false
}

// runFlushLoop starts background workers which periodically flush objects to the blobstor.
func (c *cache) runFlushLoop() {
	for i := range c.workersCount {
		c.wg.Add(1)
		go c.flushWorker(i)
	}

	c.wg.Add(1)
	go c.flushScheduler()
}

func (c *cache) flushScheduler() {
	defer c.wg.Done()

	var b *batch

	for {
		select {
		case <-c.flushErrCh:
			c.log.Warn("flush scheduler paused due to error", zap.Duration("delay", defaultErrorDelay))
			time.Sleep(defaultErrorDelay)
			for len(c.flushErrCh) > 0 {
				<-c.flushErrCh
			}
		case <-c.closeCh:
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}

		if c.objCounters.Size() == 0 {
			continue
		}

		type addrSize struct {
			addr oid.Address
			size uint64
		}
		var sortedAddrs []addrSize

		addrs := c.objCounters.Map()
		for addr, size := range addrs {
			if _, loaded := c.flushObjs.Load(addr); loaded {
				continue
			}
			sortedAddrs = append(sortedAddrs, addrSize{addr, size})
		}
		sort.Slice(sortedAddrs, func(i, j int) bool {
			return sortedAddrs[i].size > sortedAddrs[j].size
		})

	addrLoop:
		for _, as := range sortedAddrs {
			select {
			case <-c.flushErrCh:
				select {
				case c.flushErrCh <- struct{}{}:
				default:
				}
				break addrLoop
			case <-c.closeCh:
				return
			default:
			}

			if as.size > c.maxFlushBatchThreshold {
				select {
				case <-c.flushErrCh:
					select {
					case c.flushErrCh <- struct{}{}:
					default:
					}
					break addrLoop
				case c.flushCh <- as.addr:
					c.flushObjs.Store(as.addr, struct{}{})
					continue
				case <-c.closeCh:
					return
				}
			}

			var added, done bool
			for !added {
				if b == nil {
					b = c.newBatch()
				}
				added, done = b.addObject(as.addr, as.size)
				if done {
					b = nil
				}
			}
			c.flushObjs.Store(as.addr, struct{}{})
		}
	}
}

func (c *cache) flushWorker(id int) {
	defer c.wg.Done()
	for {
		select {
		case addr, ok := <-c.flushCh:
			if !ok {
				return
			}
			c.modeMtx.RLock()
			if c.readOnly() {
				c.modeMtx.RUnlock()
				continue
			}
			err := c.flushSingle(addr, true)
			c.modeMtx.RUnlock()

			c.flushObjs.Delete(addr)
			if err != nil {
				select {
				case c.flushErrCh <- struct{}{}:
				default:
				}
				c.log.Error("worker can't flush an object due to error",
					zap.Int("worker", id),
					zap.Stringer("addr", addr),
					zap.Error(err))
			}
		case <-c.closeCh:
			return
		}
	}
}

func (c *cache) reportFlushError(msg string, addr string, err error) {
	if c.reportError != nil {
		c.reportError(fmt.Sprintf("%s: %s", msg, addr), err)
	} else {
		c.log.Error(msg,
			zap.String("address", addr),
			zap.Error(err))
	}
}

func (c *cache) flushSingle(addr oid.Address, ignoreErrors bool) error {
	if c.metrics.mr != nil {
		defer elapsed(c.metrics.AddWCFlushSingleDuration)()
	}
	data, err := c.getObject(addr)
	if err != nil {
		if ignoreErrors {
			return nil
		}
		return err
	}

	err = c.storage.Put(addr, data)
	if err != nil {
		if !errors.Is(err, common.ErrNoSpace) && !errors.Is(err, common.ErrReadOnly) {
			c.reportFlushError("can't flush an object to blobstor",
				addr.EncodeToString(), err)
		}
		return err
	}

	err = c.delete(addr)
	if err != nil && !errors.As(err, new(apistatus.ObjectNotFound)) {
		c.log.Error("can't remove object from write-cache", zap.Error(err))
	}

	return nil
}

func (c *cache) flushBatch(addrs []oid.Address) error {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()

	defer func() {
		// Irrespective of the outcome these objects are no longer being processed.
		for _, addr := range addrs {
			c.flushObjs.Delete(addr)
		}
	}()

	if c.readOnly() {
		return nil
	}

	if c.metrics.mr != nil {
		defer elapsed(c.metrics.AddWCFlushBatchDuration)()
	}

	objs := make(map[oid.Address][]byte, len(addrs))
	for _, addr := range addrs {
		data, err := c.getObject(addr)
		if err != nil {
			// Continue flushing other objects.
			continue
		}

		objs[addr] = data
	}

	err := c.storage.PutBatch(objs)
	if err != nil {
		if !errors.Is(err, common.ErrNoSpace) && !errors.Is(err, common.ErrReadOnly) {
			for addr := range objs {
				c.reportFlushError("can't flush an object to blobstor",
					addr.EncodeToString(), err)
			}
		}
		return err
	}

	for addr := range objs {
		err = c.delete(addr)
		if err != nil && !errors.As(err, new(apistatus.ObjectNotFound)) {
			c.log.Error("can't remove object from write-cache", zap.Error(err))
		}
	}
	return err
}

func (c *cache) getObject(addr oid.Address) ([]byte, error) {
	sAddr := addr.EncodeToString()

	data, err := c.fsTree.GetBytes(addr)
	if err != nil {
		if errors.As(err, new(apistatus.ObjectNotFound)) {
			// an object can be removed b/w iterating over it
			// and reading its payload; not an error
			return nil, nil
		}

		c.reportFlushError("can't read a file", sAddr, err)
		return nil, err
	}

	return data, nil
}

// Flush flushes all objects from the write-cache to the main storage.
// Write-cache must be in readonly mode to ensure correctness of an operation and
// to prevent interference with background flush workers.
func (c *cache) Flush(ignoreErrors bool) error {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()

	return c.flush(ignoreErrors)
}

func (c *cache) flush(ignoreErrors bool) error {
	return c.fsTree.IterateAddresses(func(addr oid.Address) error {
		return c.flushSingle(addr, ignoreErrors)
	}, ignoreErrors)
}
