package writecache

import (
	"sort"
	"time"

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

			c.mtx.Lock()
			n := copy(c.mem, c.mem[len(m):])
			c.mem = c.mem[:n]
			for i := range c.mem {
				c.curMemSize += uint64(len(c.mem[i].data))
			}
			c.mtx.Unlock()

			sz := 0
			for i := range m {
				sz += len(m[i].addr) + m[i].obj.ToV2().StableSize()
			}
			c.dbSize.Add(uint64(sz))

		case <-c.closeCh:
			return
		}
	}
}

func (c *cache) persistToCache(objs []objectInfo) []int {
	var (
		failMem []int
		doneMem []int
	)

	_ = c.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		for i := range objs {
			if uint64(len(objs[i].data)) >= c.smallObjectSize {
				failMem = append(failMem, i)
				continue
			}

			err := b.Put([]byte(objs[i].addr), objs[i].data)
			if err != nil {
				return err
			}

			doneMem = append(doneMem, i)
		}
		return nil
	})

	if len(doneMem) > 0 {
		c.evictObjects(len(doneMem))
		for _, i := range doneMem {
			c.flushed.Add(objs[i].addr, true)
		}
	}

	var failDisk []int

	for _, i := range failMem {
		if uint64(len(objs[i].data)) > c.maxObjectSize {
			failDisk = append(failDisk, i)
			continue
		}

		err := c.fsTree.Put(objs[i].obj.Address(), objs[i].data)
		if err != nil {
			failDisk = append(failDisk, i)
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
