package writecache

import (
	"errors"
	"fmt"
	"time"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

const (
	// defaultFlushInterval is default time interval between successive flushes.
	defaultFlushInterval = 10 * time.Second
	// defaultErrorDelay is default time delay for retrying a flush after receiving an error.
	defaultErrorDelay = 10 * time.Second
	// defaultWorkerCount is a default number of workers that flush objects.
	defaultWorkerCount = 20
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
	ticker := time.NewTicker(defaultFlushInterval)

	for {
		select {
		case <-c.flushErrCh:
			c.log.Warn("flush scheduler paused due to error",
				zap.Duration("delay", defaultErrorDelay))

			time.Sleep(defaultErrorDelay)

			for len(c.flushErrCh) > 0 {
				<-c.flushErrCh
			}
		case <-ticker.C:
			c.modeMtx.RLock()
			if c.readOnly() {
				c.modeMtx.RUnlock()
				continue
			}
			c.modeMtx.RUnlock()
			err := c.fsTree.IterateAddresses(func(addr oid.Address) error {
				select {
				case <-c.flushErrCh:
					select {
					case c.flushErrCh <- struct{}{}:
					default:
					}
					return errors.New("stopping iteration due to error")
				case c.flushCh <- addr:
					return nil
				case <-c.closeCh:
					return errors.New("closed during iteration")
				}
			}, true)
			if err != nil {
				c.log.Warn("iteration failed", zap.Error(err))
			}
		case <-c.closeCh:
			return
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
	sAddr := addr.EncodeToString()

	data, err := c.fsTree.GetBytes(addr)
	if err != nil {
		if errors.As(err, new(apistatus.ObjectNotFound)) {
			// an object can be removed b/w iterating over it
			// and reading its payload; not an error
			return nil
		}

		c.reportFlushError("can't read a file", sAddr, err)
		if ignoreErrors {
			return nil
		}
		return err
	}

	var obj object.Object
	err = obj.Unmarshal(data)
	if err != nil {
		c.reportFlushError("can't unmarshal an object", sAddr, err)
		if ignoreErrors {
			return nil
		}
		return err
	}

	err = c.flushObject(&obj, data)
	if err != nil {
		return err
	}

	err = c.fsTree.Delete(addr)
	if err != nil && !errors.As(err, new(apistatus.ObjectNotFound)) {
		c.log.Error("can't remove object from write-cache", zap.Error(err))
	} else if err == nil {
		storagelog.Write(c.log,
			storagelog.AddressField(addr),
			storagelog.StorageTypeField(wcStorageType),
			storagelog.OpField("DELETE"),
		)
		c.objCounters.Delete(addr)
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
