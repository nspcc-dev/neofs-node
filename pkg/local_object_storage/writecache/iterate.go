package writecache

import (
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Iterate iterates over all objects present in write cache.
// This is very difficult to do correctly unless write-cache is put in read-only mode.
// Thus we silently fail if shard is not in read-only mode to avoid reporting misleading results.
func (c *cache) Iterate(handler func(oid.Address, []byte) error, ignoreErrors bool) error {
	c.modeMtx.RLock()
	defer c.modeMtx.RUnlock()
	if !c.readOnly() {
		return nil
	}

	var addrHandler = func(addr oid.Address) error {
		if _, ok := c.flushed.Peek(addr.EncodeToString()); ok {
			return nil
		}
		data, err := c.fsTree.GetBytes(addr)
		if err != nil {
			if ignoreErrors || errors.As(err, new(apistatus.ObjectNotFound)) {
				// an object can be removed b/w iterating over it
				// and reading its payload; not an error
				return nil
			}
			return err
		}
		return handler(addr, data)
	}

	return c.fsTree.IterateAddresses(addrHandler, ignoreErrors)
}
