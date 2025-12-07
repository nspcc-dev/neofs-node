package writecache

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
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

	var tick = time.NewTicker(defaultMaxBatchDelay)

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
		case <-tick.C:
		}

		if c.objCounters.Size() == 0 {
			continue
		}

		var sortedAddrs []oid.Address
		addrs := c.objCounters.Map()
		for addr := range addrs {
			if _, loaded := c.flushObjs.Load(addr); loaded {
				continue
			}
			sortedAddrs = append(sortedAddrs, addr)
		}
		slices.SortFunc(sortedAddrs, func(a, b oid.Address) int {
			return cmp.Compare(addrs[a], addrs[b])
		})

		var (
			b  = sortedAddrs[:0]
			bs uint64
		)
	addrLoop:
		for i, addr := range sortedAddrs {
			var (
				handledAddr bool
				flushB      bool
			)

			c.flushObjs.Store(addr, struct{}{})

			flushB = addrs[addr] > c.maxFlushBatchThreshold && len(b) != 0
			for !handledAddr || flushB {
				if flushB {
					select {
					case <-c.flushErrCh:
						select {
						case c.flushErrCh <- struct{}{}:
						default:
						}
						for _, queued := range b {
							c.flushObjs.Delete(queued)
						}
						break addrLoop
					case c.flushCh <- b:
					case <-c.closeCh:
						return
					}
					b = sortedAddrs[i:i]
					bs = 0
				}
				if handledAddr {
					break
				}
				b = b[:len(b)+1]
				bs += addrs[addr]
				handledAddr = true
				flushB = addrs[addr] > c.maxFlushBatchThreshold || len(b) >= c.maxFlushBatchCount || bs > c.maxFlushBatchSize || i == len(sortedAddrs)-1
			}
		}
	}
}

func (c *cache) flushWorker(id int) {
	defer c.wg.Done()

	for {
		var (
			addrs []oid.Address
			err   error
			ok    bool
		)

		select {
		case addrs, ok = <-c.flushCh:
		case <-c.closeCh:
			return
		}

		if !ok {
			return
		}

		c.modeMtx.RLock()
		if !c.readOnly() {
			if len(addrs) == 1 {
				err = c.flushSingle(addrs[0], true)
			} else {
				err = c.flushBatch(addrs)
			}
		}
		c.modeMtx.RUnlock()

		// Irrespective of the outcome these objects are no longer being processed.
		for _, addr := range addrs {
			c.flushObjs.Delete(addr)
		}
		if err != nil {
			select {
			case c.flushErrCh <- struct{}{}:
			default:
			}
			c.log.Error("can't flush objects",
				zap.Int("worker", id),
				zap.Stringer("first_object", addrs[0]),
				zap.Error(err))
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
		storagelog.Write(c.log,
			storagelog.AddressField(addr),
			storagelog.StorageTypeField(c.storage.Type()),
			storagelog.OpField("PUT"),
		)
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
