package writecache

import (
	"errors"
	"fmt"
	"sort"
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

	var (
		batch      []oid.Address
		batchSize  uint64
		batchTimer *time.Timer
	)

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if batchTimer != nil {
			batchTimer.Stop()
			batchTimer = nil
		}
		err := c.flushBatch(batch, true)
		if err != nil {
			c.log.Error("can't flush batch of objects", zap.Error(err))
			select {
			case c.flushErrCh <- struct{}{}:
			default:
			}
		}
		batch = nil
		batchSize = 0
		return err
	}

	checkForBatch := func() error {
		if batchTimer != nil {
			select {
			case <-batchTimer.C:
				err := flushBatch()
				if err != nil {
					return err
				}
			default:
			}
		}

		if batchSize >= c.maxFlushBatchSize || len(batch) > c.maxFlushBatchCount {
			err := flushBatch()
			if err != nil {
				return err
			}
		}

		return nil
	}

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
		}

		if c.objCounters.Size() == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		err := checkForBatch()
		if err != nil {
			continue
		}

		type addrSize struct {
			addr oid.Address
			size uint64
		}
		var sortedAddrs []addrSize

		batchSet := make(map[oid.Address]struct{}, len(batch))
		for _, bAddr := range batch {
			batchSet[bAddr] = struct{}{}
		}

		addrs := c.objCounters.Map()
		for addr, size := range addrs {
			if _, exists := batchSet[addr]; exists {
				continue
			}
			if _, loaded := c.processingBigObjs.Load(addr); loaded {
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
					c.processingBigObjs.Store(as.addr, struct{}{})
					continue
				case <-c.closeCh:
					return
				}
			}

			batch = append(batch, as.addr)
			batchSize += uint64(as.size)

			if batchTimer == nil {
				batchTimer = time.NewTimer(defaultMaxBatchDelay)
			}

			err = checkForBatch()
			if err != nil {
				continue
			}
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

			c.processingBigObjs.Delete(addr)
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

	err = c.flushObject(addr, data)
	if err != nil {
		return err
	}

	err = c.delete(addr)
	if err != nil && !errors.As(err, new(apistatus.ObjectNotFound)) {
		c.log.Error("can't remove object from write-cache", zap.Error(err))
	}

	return nil
}

// flushObject is used to write object directly to the main storage.
func (c *cache) flushObject(addr oid.Address, data []byte) error {
	err := c.storage.Put(addr, data)
	if err != nil {
		if !errors.Is(err, common.ErrNoSpace) && !errors.Is(err, common.ErrReadOnly) {
			c.reportFlushError("can't flush an object to blobstor",
				addr.EncodeToString(), err)
		}
		return err
	}

	return err
}

func (c *cache) flushBatch(addrs []oid.Address, ignoreErrors bool) error {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()

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
			if ignoreErrors {
				continue
			}
			return err
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
