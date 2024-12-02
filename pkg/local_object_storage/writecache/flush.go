package writecache

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/mr-tron/base58"
	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

const (
	// flushBatchSize is amount of keys which will be read from cache to be flushed
	// to the main storage. It is used to reduce contention between cache put
	// and cache persist.
	flushBatchSize = 512
	// defaultFlushWorkersCount is number of workers for putting objects in main storage.
	defaultFlushWorkersCount = 20
	// defaultFlushInterval is default time interval between successive flushes.
	defaultFlushInterval = time.Second
)

// runFlushLoop starts background workers which periodically flush objects to the blobstor.
func (c *cache) runFlushLoop() {
	for i := range c.workersCount {
		c.wg.Add(1)
		go c.flushWorker(i)
	}

	c.wg.Add(1)
	go c.flushBigObjects()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		tt := time.NewTimer(defaultFlushInterval)
		defer tt.Stop()

		for {
			select {
			case <-tt.C:
				c.flushDB()
				tt.Reset(defaultFlushInterval)
			case <-c.closeCh:
				return
			}
		}
	}()
}

func (c *cache) flushDB() {
	var lastKey []byte
	var m []objectInfo
	for {
		select {
		case <-c.closeCh:
			return
		default:
		}

		m = m[:0]

		c.modeMtx.RLock()
		if c.readOnly() {
			c.modeMtx.RUnlock()
			time.Sleep(time.Second)
			continue
		}

		// We put objects in batches of fixed size to not interfere with main put cycle a lot.
		_ = c.db.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(defaultBucket)
			cs := b.Cursor()

			var k, v []byte

			if len(lastKey) == 0 {
				k, v = cs.First()
			} else {
				k, v = cs.Seek(lastKey)
				if bytes.Equal(k, lastKey) {
					k, v = cs.Next()
				}
			}

			for ; k != nil && len(m) < flushBatchSize; k, v = cs.Next() {
				if len(lastKey) == len(k) {
					copy(lastKey, k)
				} else {
					lastKey = bytes.Clone(k)
				}

				m = append(m, objectInfo{
					addr: string(k),
					data: bytes.Clone(v),
				})
			}
			return nil
		})

		var count int
		for i := range m {
			if c.flushed.Contains(m[i].addr) {
				continue
			}

			obj := object.New()
			if err := obj.Unmarshal(m[i].data); err != nil {
				continue
			}

			count++
			select {
			case c.flushCh <- obj:
			case <-c.closeCh:
				c.modeMtx.RUnlock()
				return
			}
		}

		if count == 0 {
			c.modeMtx.RUnlock()
			break
		}

		c.modeMtx.RUnlock()

		c.log.Debug("tried to flush items from write-cache",
			zap.Int("count", count),
			zap.String("start", base58.Encode(lastKey)))
	}
}

func (c *cache) flushBigObjects() {
	defer c.wg.Done()

	tick := time.NewTicker(defaultFlushInterval * 10)
	for {
		select {
		case <-tick.C:
			c.modeMtx.RLock()
			if c.readOnly() {
				c.modeMtx.RUnlock()
				break
			}

			_ = c.flushFSTree(true)

			c.modeMtx.RUnlock()
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

func (c *cache) flushFSTree(ignoreErrors bool) error {
	var lazyHandler = func(addr oid.Address, f func() ([]byte, error)) error {
		sAddr := addr.EncodeToString()

		if _, ok := c.store.flushed.Peek(sAddr); ok {
			return nil
		}

		data, err := f()
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
			if ignoreErrors {
				return nil
			}
			return err
		}

		// mark object as flushed
		c.flushed.Add(sAddr, false)

		return nil
	}

	return c.fsTree.IterateLazily(lazyHandler, ignoreErrors)
}

// flushWorker writes objects to the main storage.
func (c *cache) flushWorker(_ int) {
	defer c.wg.Done()

	var obj *object.Object
	for {
		// Give priority to direct put.
		select {
		case obj = <-c.flushCh:
		case <-c.closeCh:
			return
		}

		err := c.flushObject(obj, nil)
		if err == nil {
			c.flushed.Add(objectCore.AddressOf(obj).EncodeToString(), true)
		}
	}
}

// flushObject is used to write object directly to the main storage.
func (c *cache) flushObject(obj *object.Object, data []byte) error {
	addr := objectCore.AddressOf(obj)

	var prm common.PutPrm
	prm.Object = obj
	prm.RawData = data

	res, err := c.blobstor.Put(prm)
	if err != nil {
		if !errors.Is(err, common.ErrNoSpace) && !errors.Is(err, common.ErrReadOnly) &&
			!errors.Is(err, blobstor.ErrNoPlaceFound) {
			c.reportFlushError("can't flush an object to blobstor",
				addr.EncodeToString(), err)
		}
		return err
	}

	err = c.metabase.UpdateStorageID(addr, res.StorageID)
	if err != nil {
		if errors.Is(err, apistatus.ErrObjectNotFound) {
			// we have the object and we just successfully put it so all the
			// information for restoring the object is here; meta can be
			// corrupted, resynced, etc, just trying our best
			err = c.metabase.Put(obj, res.StorageID, nil)
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
	if err := c.flushFSTree(ignoreErrors); err != nil {
		return err
	}

	return c.db.View(func(tx *bbolt.Tx) error {
		var addr oid.Address

		b := tx.Bucket(defaultBucket)
		cs := b.Cursor()
		for k, data := cs.Seek(nil); k != nil; k, data = cs.Next() {
			sa := string(k)
			if _, ok := c.flushed.Peek(sa); ok {
				continue
			}

			if err := addr.DecodeString(sa); err != nil {
				c.reportFlushError("can't decode object address from the DB", sa, err)
				if ignoreErrors {
					continue
				}
				return err
			}

			var obj object.Object
			if err := obj.Unmarshal(data); err != nil {
				c.reportFlushError("can't unmarshal an object from the DB", sa, err)
				if ignoreErrors {
					continue
				}
				return err
			}

			if err := c.flushObject(&obj, data); err != nil {
				return err
			}
		}
		return nil
	})
}
