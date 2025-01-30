package writecache

import (
	"fmt"
	"math"
	"sync/atomic"
)

func (c *cache) estimateCacheSize() uint64 {
	return c.objCounters.FS() * c.maxObjectSize
}

func (c *cache) incSizeFS(sz uint64) uint64 {
	return sz + c.maxObjectSize
}

type counters struct {
	cFS atomic.Uint64
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
	inFS, err := c.fsTree.NumberOfObjects()
	if err != nil {
		return fmt.Errorf("could not read write-cache FS counter: %w", err)
	}

	c.objCounters.cFS.Store(inFS)

	return nil
}
