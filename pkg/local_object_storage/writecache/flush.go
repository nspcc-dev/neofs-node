package writecache

import (
	"sync"
	"time"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"go.uber.org/zap"
)

const (
	// flushBatchSize is amount of keys which will be read from cache to be flushed
	// to the main storage. It is used to reduce contention between cache put
	// and cache persist.
	flushBatchSize = 512
	// flushWorkersCount is number of workers for putting objects in main storage.
	flushWorkersCount = 20
	// defaultFlushInterval is default time interval between successive flushes.
	defaultFlushInterval = time.Second
)

// flushLoop periodically flushes changes from the database to memory.
func (c *cache) flushLoop() {
	var wg sync.WaitGroup

	for i := 0; i < c.workersCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.flushWorker(i)
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.flushBigObjects()
	}()

	tick := time.NewTicker(defaultFlushInterval)
	for {
		select {
		case <-tick.C:
			c.flush()
		case <-c.closeCh:
			c.log.Debug("waiting for workers to quit")
			wg.Wait()
			return
		}
	}
}

func (c *cache) flush() {
	iter := c.db.NewIter(nil)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		k := string(iter.Key())
		if _, ok := c.flushed.Peek(k); ok {
			continue
		}

		obj := object.New()
		if err := obj.Unmarshal(iter.Value()); err != nil {
			continue
		}

		select {
		case c.flushCh <- obj:
		case <-c.closeCh:
			return
		}

		c.evictObjects(1)
		c.flushed.Add(k, true)
	}
}

func (c *cache) flushBigObjects() {
	tick := time.NewTicker(defaultFlushInterval * 10)
	for {
		select {
		case <-tick.C:
			_ = c.fsTree.Iterate(func(addr *objectSDK.Address, data []byte) error {
				if _, ok := c.store.flushed.Peek(addr.String()); ok {
					return nil
				}

				if _, err := c.blobstor.PutRaw(addr, data); err != nil {
					c.log.Error("cant flush object to blobstor", zap.Error(err))
				}
				return nil
			})
		case <-c.closeCh:
		}
	}
}

// flushWorker runs in a separate goroutine and write objects to the main storage.
// If flushFirst is true, flushing objects from cache database takes priority over
// putting new objects.
func (c *cache) flushWorker(num int) {
	priorityCh := c.directCh
	switch num % 3 {
	case 0:
		priorityCh = c.flushCh
	case 1:
		priorityCh = c.metaCh
	}

	var obj *object.Object
	for {
		metaOnly := false

		// Give priority to direct put.
		// TODO(fyrchik): do this once in N iterations depending on load
		select {
		case obj = <-priorityCh:
		default:
			select {
			case obj = <-c.directCh:
			case obj = <-c.flushCh:
			case obj = <-c.metaCh:
				metaOnly = true
			case <-c.closeCh:
				return
			}
		}

		err := c.writeObject(obj, metaOnly)
		if err != nil {
			c.log.Error("can't flush object to the main storage", zap.Error(err))
		}
	}
}

// writeObject is used to write object directly to the main storage.
func (c *cache) writeObject(obj *object.Object, metaOnly bool) error {
	var id *blobovnicza.ID

	if !metaOnly {
		prm := new(blobstor.PutPrm)
		prm.SetObject(obj)
		res, err := c.blobstor.Put(prm)
		if err != nil {
			return err
		}

		id = res.BlobovniczaID()
	}

	return meta.Put(c.metabase, obj, id)
}

func cloneBytes(a []byte) []byte {
	b := make([]byte, len(a))
	copy(b, a)
	return b
}
