package writecache

import (
	"fmt"
	"sync"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type counters struct {
	mu     sync.Mutex
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
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.size
}

func (c *cache) initCounters() error {
	var sizeHandler = func(addr oid.Address, size uint64) error {
		c.objCounters.Add(addr, size)
		return nil
	}

	err := c.fsTree.IterateSizes(sizeHandler, false)
	if err != nil {
		return fmt.Errorf("could not read write-cache FS counter: %w", err)
	}

	return nil
}
