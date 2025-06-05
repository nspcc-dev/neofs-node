package writecache

import (
	"fmt"
	"maps"
	"sync"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type counters struct {
	mu     sync.RWMutex
	objMap map[oid.Address]uint64
	size   uint64
}

func (x *counters) Add(addr oid.Address, size uint64) {
	x.mu.Lock()
	defer x.mu.Unlock()

	x.size += size
	x.objMap[addr] = size
}

func (x *counters) Delete(addr oid.Address) {
	x.mu.Lock()
	defer x.mu.Unlock()

	x.size -= x.objMap[addr]
	delete(x.objMap, addr)
}

func (x *counters) Size() uint64 {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return x.size
}

func (x *counters) HasAddress(addr oid.Address) bool {
	x.mu.RLock()
	defer x.mu.RUnlock()
	_, ok := x.objMap[addr]
	return ok
}

func (x *counters) Map() map[oid.Address]uint64 {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return maps.Clone(x.objMap)
}

func (c *cache) initCounters() error {
	var sizeHandler = func(addr oid.Address, size uint64) error {
		if _, ok := c.objCounters.objMap[addr]; ok {
			return nil
		}
		c.objCounters.Add(addr, size)
		c.metrics.IncWCObjectCount()
		c.metrics.AddWCSize(size)
		return nil
	}

	err := c.fsTree.IterateSizes(sizeHandler, false)
	if err != nil {
		return fmt.Errorf("could not read write-cache FS counter: %w", err)
	}

	return nil
}
