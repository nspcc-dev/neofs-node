package writecache

import (
	"fmt"
	"math"
	"sync/atomic"

	"go.etcd.io/bbolt"
)

func (c *cache) estimateCacheSize() uint64 {
	return c.objCounters.DB()*c.smallObjectSize + c.objCounters.FS()*c.maxObjectSize
}

func (c *cache) incSizeDB(sz uint64) uint64 {
	return sz + c.smallObjectSize
}

func (c *cache) incSizeFS(sz uint64) uint64 {
	return sz + c.maxObjectSize
}

type counters struct {
	cDB, cFS atomic.Uint64
}

func (x *counters) IncDB() {
	x.cDB.Add(1)
}

func (x *counters) DecDB() {
	x.cDB.Add(math.MaxUint32)
}

func (x *counters) DB() uint64 {
	return x.cDB.Load()
}

func (x *counters) IncFS() {
	x.cFS.Add(1)
}

func (x *counters) DecFS() {
	x.cFS.Add(math.MaxUint32)
}

func (x *counters) FS() uint64 {
	return x.cFS.Load()
}

func (c *cache) initCounters() error {
	var inDB uint64
	err := c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b != nil {
			inDB = uint64(b.Stats().KeyN)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not read write-cache DB counter: %w", err)
	}

	inFS, err := c.fsTree.NumberOfObjects()
	if err != nil {
		return fmt.Errorf("could not read write-cache FS counter: %w", err)
	}

	c.objCounters.cDB.Store(inDB)
	c.objCounters.cFS.Store(inFS)

	return nil
}
