package meta

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"go.etcd.io/bbolt"
)

// Get returns object header for specified address.
func (db *DB) Get(addr *objectSDK.Address) (*object.Object, error) {
	var obj *object.Object

	if err := db.boltDB.View(func(tx *bbolt.Tx) error {
		addrKey := addressKey(addr)

		// check if object marked as deleted
		if objectRemoved(tx, addrKey) {
			return object.ErrNotFound
		}

		primaryBucket := tx.Bucket(primaryBucket)
		if primaryBucket == nil {
			return object.ErrNotFound
		}

		data := primaryBucket.Get(addrKey)
		if data == nil {
			return object.ErrNotFound
		}

		var err error

		obj = object.New()

		err = obj.Unmarshal(data)

		return err
	}); err != nil {
		return nil, err
	}

	return obj, nil
}
