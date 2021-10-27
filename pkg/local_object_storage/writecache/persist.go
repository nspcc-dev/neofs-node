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
			c.persistSmallObjects(m)
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

// persistSmallObjects persists small objects to the write-cache database and
// pushes the to the flush workers queue.
func (c *cache) persistSmallObjects(objs []objectInfo) {
	cacheSize := c.estimateCacheSize()
	overflowIndex := len(objs)
	for i := range objs {
		newSize := c.incSizeDB(cacheSize)
		if c.maxCacheSize < newSize {
			overflowIndex = i
			break
		}
		cacheSize = newSize
	}

	err := c.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		for i := 0; i < overflowIndex; i++ {
			err := b.Put([]byte(objs[i].addr), objs[i].data)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		overflowIndex = 0
	} else {
		c.evictObjects(overflowIndex)
	}

	for i := 0; i < overflowIndex; i++ {
		storagelog.Write(c.log, storagelog.AddressField(objs[i].addr), storagelog.OpField("db PUT"))
		c.objCounters.IncDB()
		c.flushed.Add(objs[i].addr, true)
	}

	c.addToFlushQueue(objs, overflowIndex)
}

// persistBigObject writes object to FSTree and pushes it to the flush workers queue.
func (c *cache) persistBigObject(objInfo objectInfo) {
	cacheSz := c.estimateCacheSize()
	metaIndex := 0
	if c.incSizeFS(cacheSz) <= c.maxCacheSize {
		err := c.fsTree.Put(objInfo.obj.Address(), objInfo.data)
		if err == nil {
			metaIndex = 1
			c.objCounters.IncFS()
			storagelog.Write(c.log, storagelog.AddressField(objInfo.addr), storagelog.OpField("fstree PUT"))
		}
	}
	c.addToFlushQueue([]objectInfo{objInfo}, metaIndex)
}

// addToFlushQueue pushes objects to the flush workers queue.
// For objects below metaIndex only meta information will be flushed.
func (c *cache) addToFlushQueue(objs []objectInfo, metaIndex int) {
	for i := 0; i < metaIndex; i++ {
		select {
		case c.metaCh <- objs[i].obj:
		case <-c.closeCh:
			return
		}
	}
	for i := metaIndex; i < len(objs); i++ {
		select {
		case c.directCh <- objs[i].obj:
		case <-c.closeCh:
			return
		}
	}
}
