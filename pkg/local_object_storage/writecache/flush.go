package writecache

import (
	"errors"
	"fmt"
	"sort"
	"time"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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

		batchSet := make(map[string]struct{}, len(batch))
		for _, bAddr := range batch {
			batchSet[bAddr.String()] = struct{}{}
		}

		addrs := c.objCounters.Map()
		for addr, size := range addrs {
			if _, exists := batchSet[addr.String()]; exists {
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
					break
				case c.flushCh <- as.addr:
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
	obj, data, err := c.getObject(addr)
	if err != nil {
		if ignoreErrors {
			return nil
		}
		return err
	}
	if obj == nil {
		return nil
	}

	err = c.flushObject(obj, data)
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
func (c *cache) flushObject(obj *object.Object, data []byte) error {
	addr := objectCore.AddressOf(obj)

	sid, err := c.blobstor.Put(addr, obj, data)
	if err != nil {
		if !errors.Is(err, common.ErrNoSpace) && !errors.Is(err, common.ErrReadOnly) &&
			!errors.Is(err, blobstor.ErrNoPlaceFound) {
			c.reportFlushError("can't flush an object to blobstor",
				addr.EncodeToString(), err)
		}
		return err
	}

	err = c.metabase.UpdateStorageID(addr, sid)
	if err != nil {
		if errors.Is(err, apistatus.ErrObjectNotFound) {
			// we have the object and we just successfully put it so all the
			// information for restoring the object is here; meta can be
			// corrupted, resynced, etc, just trying our best
			err = c.metabase.Put(obj, sid, nil)
			if err != nil {
				err = fmt.Errorf("trying to restore missing object in metabase: %w", err)
			}

			return err
		}
		c.reportFlushError("can't update object storage ID",
			addr.EncodeToString(), err)
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

	objs := make([]blobstor.PutBatchPrm, 0, len(addrs))
	for _, addr := range addrs {
		obj, data, err := c.getObject(addr)
		if err != nil {
			if ignoreErrors {
				continue
			}
			return err
		}
		if obj == nil {
			continue
		}

		objs = append(objs, blobstor.PutBatchPrm{
			Addr: addr,
			Obj:  obj,
			Raw:  data,
		})
	}

	sid, err := c.blobstor.PutBatch(objs)
	if err != nil {
		if !errors.Is(err, common.ErrNoSpace) && !errors.Is(err, common.ErrReadOnly) &&
			!errors.Is(err, blobstor.ErrNoPlaceFound) {
			for _, obj := range objs {
				c.reportFlushError("can't flush an object to blobstor",
					obj.Addr.EncodeToString(), err)
			}
		}
		return err
	}

	for _, obj := range objs {
		err = c.metabase.UpdateStorageID(obj.Addr, sid)
		if err != nil {
			if errors.Is(err, apistatus.ErrObjectNotFound) {
				// we have the object and we just successfully put it so all the
				// information for restoring the object is here; meta can be
				// corrupted, resynced, etc, just trying our best
				err = c.metabase.Put(obj.Obj, sid, nil)
				if err != nil {
					c.log.Error("trying to restore missing object in metabase", zap.Error(err))
				}
			}
			c.reportFlushError("can't update object storage ID",
				obj.Addr.EncodeToString(), err)
		}

		err = c.delete(obj.Addr)
		if err != nil && !errors.As(err, new(apistatus.ObjectNotFound)) {
			c.log.Error("can't remove object from write-cache", zap.Error(err))
		}
	}
	return err
}

func (c *cache) getObject(addr oid.Address) (*object.Object, []byte, error) {
	sAddr := addr.EncodeToString()

	data, err := c.fsTree.GetBytes(addr)
	if err != nil {
		if errors.As(err, new(apistatus.ObjectNotFound)) {
			// an object can be removed b/w iterating over it
			// and reading its payload; not an error
			return nil, nil, nil
		}

		c.reportFlushError("can't read a file", sAddr, err)
		return nil, nil, err
	}

	var obj object.Object
	err = obj.Unmarshal(data)
	if err != nil {
		c.reportFlushError("can't unmarshal an object", sAddr, err)
		return nil, nil, err
	}

	return &obj, data, nil
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
