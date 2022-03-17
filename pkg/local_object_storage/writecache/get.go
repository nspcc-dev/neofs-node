package writecache

import (
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.etcd.io/bbolt"
)

// Get returns object from write-cache.
//
// Returns apistatus.ObjectNotFound if requested object is missing in write-cache.
func (c *cache) Get(addr *addressSDK.Address) (*objectSDK.Object, error) {
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
// Returns apistatus.ObjectNotFound if requested object is missing in write-cache.
func (c *cache) Head(addr *addressSDK.Address) (*objectSDK.Object, error) {
	// TODO: #1149 easiest to implement solution is presented here, consider more efficient way, e.g.:
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
	return obj.CutPayload(), nil
}

// Get fetches object from the underlying database.
// Key should be a stringified address.
//
// Returns apistatus.ObjectNotFound if requested object is missing in db.
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
