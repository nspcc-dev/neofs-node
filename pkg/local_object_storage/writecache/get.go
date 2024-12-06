package writecache

import (
	"bytes"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// Get returns object from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in write-cache.
func (c *cache) Get(addr oid.Address) (*objectSDK.Object, error) {
	saddr := addr.EncodeToString()

	value, err := Get(c.db, []byte(saddr))
	if err == nil {
		obj := objectSDK.New()
		c.flushed.Get(saddr)
		return obj, obj.Unmarshal(value)
	}

	obj, err := c.fsTree.Get(addr)
	if err != nil {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	c.flushed.Get(saddr)
	return obj, nil
}

// Head returns object header from write-cache.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in write-cache.
func (c *cache) Head(addr oid.Address) (*objectSDK.Object, error) {
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
	return get(db, key)
}

func get(db *bbolt.DB, key []byte) ([]byte, error) {
	var value []byte
	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		if b == nil {
			return ErrNoDefaultBucket
		}
		v := b.Get(key)
		if v == nil {
			return logicerr.Wrap(apistatus.ObjectNotFound{})
		}
		value = bytes.Clone(v)
		return nil
	})
	return value, err
}

func (c *cache) GetBytes(addr oid.Address) ([]byte, error) {
	saddr := addr.EncodeToString()
	b, err := get(c.db, []byte(saddr))
	if err == nil {
		c.flushed.Get(saddr)
		return b, nil
	}

	b, err = c.fsTree.GetBytes(addr)
	if err != nil {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	c.flushed.Get(saddr)
	return b, nil
}
