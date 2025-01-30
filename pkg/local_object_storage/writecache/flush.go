package writecache

import (
	"errors"
	"fmt"
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
	// defaultFlushInterval is default time interval between successive flushes.
	defaultFlushInterval = 10 * time.Second
)

// runFlushLoop starts background workers which periodically flush objects to the blobstor.
func (c *cache) runFlushLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		tick := time.NewTicker(defaultFlushInterval)
		for {
			select {
			case <-tick.C:
				c.modeMtx.RLock()
				if c.readOnly() {
					c.modeMtx.RUnlock()
					break
				}

				_ = c.flush(true)

				c.modeMtx.RUnlock()
			case <-c.closeCh:
				return
			}
		}
	}()
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

func (c *cache) flush(ignoreErrors bool) error {
	var addrHandler = func(addr oid.Address) error {
		sAddr := addr.EncodeToString()

		if _, ok := c.store.flushed.Peek(sAddr); ok {
			return nil
		}

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

		// mark object as flushed
		c.flushed.Add(sAddr, false)

		return nil
	}

	return c.fsTree.IterateAddresses(addrHandler, ignoreErrors)
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
