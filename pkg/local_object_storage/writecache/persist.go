package writecache

import (
	"sort"
	"time"

	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

const defaultPersistInterval = time.Second

// persistLoop persists object accumulated in memory to the database.
func (c *cache) persistLoop() {
	tick := time.NewTicker(defaultPersistInterval)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			c.mtx.RLock()
			m := c.mem
			c.mtx.RUnlock()

			sort.Slice(m, func(i, j int) bool { return m[i].addr < m[j].addr })

			start := time.Now()
			c.persistObjects(m)
			c.log.Debug("persisted items to disk",
				zap.Duration("took", time.Since(start)),
				zap.Int("total", len(m)))

			for i := range m {
				storagelog.Write(c.log,
					storagelog.AddressField(m[i].addr),
					storagelog.OpField("in-mem DELETE persist"),
				)
			}

			c.mtx.Lock()
			c.curMemSize = 0
			n := copy(c.mem, c.mem[len(m):])
			c.mem = c.mem[:n]
			for i := range c.mem {
				c.curMemSize += uint64(len(c.mem[i].data))
			}
			c.mtx.Unlock()
		case <-c.closeCh:
			return
		}
	}
}

func (c *cache) persistToCache(objs []objectInfo) []int {
	var (
		failMem []int // some index is negative => all objects starting from it will overflow the cache
		doneMem []int
	)
	var sz uint64
	err := c.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		cacheSz := c.estimateCacheSize()
		for i := range objs {
			if uint64(len(objs[i].data)) >= c.smallObjectSize {
				failMem = append(failMem, i)
				continue
			}

			// check if object will overflow write-cache size limit
			updCacheSz := c.incSizeDB(cacheSz)
			if updCacheSz > c.maxCacheSize {
				// set negative index. We decrement index to cover 0 val (overflow is practically impossible)
				failMem = append(failMem, -i-1)

				return nil
			}

			err := b.Put([]byte(objs[i].addr), objs[i].data)
			if err != nil {
				return err
			}
			sz += uint64(len(objs[i].data))
			doneMem = append(doneMem, i)
			storagelog.Write(c.log, storagelog.AddressField(objs[i].addr), storagelog.OpField("db PUT"))

			// update cache size
			cacheSz = updCacheSz
			c.objCounters.IncDB()
		}
		return nil
	})
	if err == nil {
		c.dbSize.Add(sz)
	}
	if len(doneMem) > 0 {
		c.evictObjects(len(doneMem))
		for _, i := range doneMem {
			c.flushed.Add(objs[i].addr, true)
		}
	}

	var failDisk []int

	cacheSz := c.estimateCacheSize()

	for _, objInd := range failMem {
		var (
			updCacheSz  uint64
			overflowInd = -1
		)

		if objInd < 0 {
			// actually, since the overflow was detected in DB tx, the required space could well have been freed,
			// but it is easier to consider the entire method atomic
			overflowInd = -objInd - 1 // subtract 1 since we decremented index above
		} else {
			// check if object will overflow write-cache size limit
			if updCacheSz = c.incSizeFS(cacheSz); updCacheSz > c.maxCacheSize {
				overflowInd = objInd
			}
		}

		if overflowInd >= 0 {
		loop:
			for j := range objs[overflowInd:] {
				// exclude objects which are already stored in DB
				for _, doneMemInd := range doneMem {
					if j == doneMemInd {
						continue loop
					}
				}

				failDisk = append(failDisk, j)
			}

			break
		}

		if uint64(len(objs[objInd].data)) > c.maxObjectSize {
			failDisk = append(failDisk, objInd)
			continue
		}

		err := c.fsTree.Put(objs[objInd].obj.Address(), objs[objInd].data)
		if err != nil {
			failDisk = append(failDisk, objInd)
		} else {
			storagelog.Write(c.log, storagelog.AddressField(objs[objInd].addr), storagelog.OpField("fstree PUT"))

			// update cache size
			cacheSz = updCacheSz
			c.objCounters.IncFS()
		}
	}

	return failDisk
}

// persistObjects tries to write objects from memory to the persistent storage.
// If tryCache is false, writing skips cache and is done directly to the main storage.
func (c *cache) persistObjects(objs []objectInfo) {
	toDisk := c.persistToCache(objs)
	j := 0

	for i := range objs {
		ch := c.metaCh
		if j < len(toDisk) {
			if i == toDisk[j] {
				ch = c.directCh
			} else {
				for ; j < len(toDisk) && i > toDisk[j]; j++ {
				}
			}
		}

		select {
		case ch <- objs[j].obj:
		case <-c.closeCh:
			return
		}
	}
}
