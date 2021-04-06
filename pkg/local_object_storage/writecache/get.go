package writecache

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"go.etcd.io/bbolt"
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

	var value []byte
	_ = c.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		val := b.Get([]byte(saddr))
		if val != nil {
			value = cloneBytes(val)
		}
		return nil
	})

	if value != nil {
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
