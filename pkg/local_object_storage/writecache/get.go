package writecache

import (
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.etcd.io/bbolt"
)

// Get returns object from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in write-cache.
func (c *cache) Get(addr *addressSDK.Address) (*objectSDK.Object, error) {
	saddr := addr.String()

	c.mtx.RLock()
	for i := range c.mem {
		if saddr == c.mem[i].addr {
			data := c.mem[i].data
			c.mtx.RUnlock()
			// We unmarshal object instead of using cached value to avoid possibility
			// of unintentional object corruption by caller.
			// It is safe to unmarshal without mutex, as storage under `c.mem[i].data` slices is not reused.
			obj := objectSDK.New()
			return obj, obj.Unmarshal(data)
		}
	}
	c.mtx.RUnlock()

	value, err := Get(c.db, []byte(saddr))
	if err == nil {
		obj := objectSDK.New()
		c.flushed.Get(saddr)
		return obj, obj.Unmarshal(value)
	}

	data, err := c.fsTree.Get(addr)
	if err != nil {
		var errNotFound apistatus.ObjectNotFound

		return nil, errNotFound
	}

	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return nil, err
	}

	c.flushed.Get(saddr)
	return obj, nil
}

// Head returns object header from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in write-cache.
func (c *cache) Head(addr *addressSDK.Address) (*objectSDK.Object, error) {
	obj, err := c.Get(addr)
	if err != nil {
		return nil, err
	}

	return obj.CutPayload(), nil
}

// Get fetches object from the underlying database.
// Key should be a stringified address.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in db.
func Get(db *bbolt.DB, key []byte) ([]byte, error) {
	var value []byte
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return ErrNoDefaultBucket
		}
		value = b.Get(key)
		if value == nil {
			var errNotFound apistatus.ObjectNotFound

			return errNotFound
		}
		value = cloneBytes(value)
		return nil
	})
	return value, err
}
