package writecache

import (
	"time"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
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
	for i := 0; i < c.workersCount; i++ {
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
				c.flush()
				tt.Reset(defaultFlushInterval)
			case <-c.closeCh:
				return
			}
		}
	}()
}

func (c *cache) flush() {
	lastKey := []byte{}
	var m []objectInfo
	for {
		select {
		case <-c.closeCh:
			return
		default:
		}

		m = m[:0]
		sz := 0

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
			for k, v := cs.Seek(lastKey); k != nil && len(m) < flushBatchSize; k, v = cs.Next() {
				if _, ok := c.flushed.Peek(string(k)); ok {
					continue
				}

				sz += len(k) + len(v)
				m = append(m, objectInfo{
					addr: string(k),
					data: slice.Copy(v),
				})
			}
			return nil
		})

		for i := range m {
			obj := object.New()
			if err := obj.Unmarshal(m[i].data); err != nil {
				continue
			}

			select {
			case c.flushCh <- obj:
			case <-c.closeCh:
				c.modeMtx.RUnlock()
				return
			}
		}

		if len(m) == 0 {
			c.modeMtx.RUnlock()
			break
		}

		c.modeMtx.RUnlock()

		c.log.Debug("tried to flush items from write-cache",
			zap.Int("count", len(m)),
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

			evictNum := 0

			var prm common.IteratePrm
			prm.LazyHandler = func(addr oid.Address, f func() ([]byte, error)) error {
				sAddr := addr.EncodeToString()

				if _, ok := c.store.flushed.Peek(sAddr); ok {
					return nil
				}

				data, err := f()
				if err != nil {
					c.log.Error("can't read a file", zap.Stringer("address", addr))
					return nil
				}

				c.mtx.Lock()
				_, compress := c.compressFlags[sAddr]
				c.mtx.Unlock()

				var prm common.PutPrm
				prm.Address = addr
				prm.RawData = data
				prm.DontCompress = !compress

				if _, err := c.blobstor.Put(prm); err != nil {
					c.log.Error("cant flush object to blobstor", zap.Error(err))
					return nil
				}

				if compress {
					c.mtx.Lock()
					delete(c.compressFlags, sAddr)
					c.mtx.Unlock()
				}

				// mark object as flushed
				c.flushed.Add(sAddr, false)

				evictNum++

				return nil
			}

			_, _ = c.fsTree.Iterate(prm)

			c.modeMtx.RUnlock()
		case <-c.closeCh:
			return
		}
	}
}

// flushWorker runs in a separate goroutine and write objects to the main storage.
// If flushFirst is true, flushing objects from cache database takes priority over
// putting new objects.
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

		err := c.flushObject(obj)
		if err != nil {
			c.log.Error("can't flush object to the main storage", zap.Error(err))
		} else {
			c.flushed.Add(objectCore.AddressOf(obj).EncodeToString(), true)
		}
	}
}

// flushObject is used to write object directly to the main storage.
func (c *cache) flushObject(obj *object.Object) error {
	var prm common.PutPrm
	prm.Object = obj

	res, err := c.blobstor.Put(prm)
	if err != nil {
		return err
	}

	var pPrm meta.PutPrm
	pPrm.SetObject(obj)
	pPrm.SetStorageID(res.StorageID)

	_, err = c.metabase.Put(pPrm)
	return err
}
