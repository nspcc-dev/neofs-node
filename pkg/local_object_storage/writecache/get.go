package writecache

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
)

// Get returns object from write-cache.
func (c *cache) Get(addr *objectSDK.Address) (*object.Object, error) {
	saddr := addr.String()

	c.mtx.RLock()
	for i := range c.mem {
		if saddr == c.mem[i].addr {
			obj := c.mem[i].obj
			c.mtx.RUnlock()
			return obj, nil
		}
	}
	c.mtx.RUnlock()

	value, cl, err := c.db.Get([]byte(saddr))
	if err == nil {
		defer cl.Close()

		obj := object.New()
		c.flushed.Get(saddr)
		return obj, obj.Unmarshal(value)
	}

	data, err := c.fsTree.Get(addr)
	if err != nil {
		return nil, object.ErrNotFound
	}

	obj := object.New()
	if err := obj.Unmarshal(data); err != nil {
		return nil, err
	}

	c.flushed.Get(saddr)
	return obj, nil
}

// Head returns object header from write-cache.
func (c *cache) Head(addr *objectSDK.Address) (*object.Object, error) {
	// TODO: easiest to implement solution is presented here, consider more efficient way, e.g.:
	//  - provide header as common object.Object to Put, but marked to prevent correlation with full object
	//    (all write-cache logic will automatically spread to headers, except flushing)
	//  - cut header from in-memory objects directly and persist headers into particular bucket of DB
	//    (explicit sync with full objects is needed)
	//  - try to pull out binary header from binary full object (not sure if it is possible)
	obj, err := c.Get(addr)
	if err != nil {
		return nil, err
	}

	// NOTE: resetting the payload via the setter can lead to data corruption of in-memory objects, but ok for others
	return object.NewRawFromObject(obj).CutPayload().Object(), nil
}
